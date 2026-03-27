import os
import gzip
import json
import logging
import time
import requests
from io import BytesIO
from urllib.parse import urljoin
from urllib3.exceptions import ProtocolError
from common.s3 import S3FileHandler

logging.basicConfig(level=logging.INFO)


def _get_delta_filenames(session, index_url):
    """Fetch the list of available delta filenames from the index."""
    response = session.get(index_url, timeout=30)
    response.raise_for_status()
    filenames = [line.strip() for line in response.text.splitlines() if line.strip()]
    return filenames


def _matches_country(tags, country):
    """Check if any of the countries_tags contains the given country string."""
    return isinstance(tags, list) and any(country.lower() in tag.lower() for tag in tags)


def _download_delta(session, base_url, filename, country, max_retries=3):
    """Stream a gzipped JSONL delta file and return only records matching the country.

    Retries up to max_retries times on connection drops (large files ~1GB+ are prone to this).

    TODO: Les fichiers delta très volumineux (~1.3GB+) échouent systématiquement même après les retries.
    La solution propre serait d'implémenter le téléchargement par plages HTTP (Range requests)
    pour reprendre le téléchargement là où il s'est arrêté plutôt que de repartir du début.
    En attendant, utiliser --last_processed_file pour sauter les fichiers problématiques.
    """
    url = urljoin(base_url, filename)

    for attempt in range(1, max_retries + 1):
        logging.info(f"Downloading delta file: {url} (attempt {attempt}/{max_retries})")
        try:
            records = []
            with session.get(url, timeout=300, stream=True) as response:
                response.raise_for_status()
                with gzip.GzipFile(fileobj=response.raw) as gz:
                    for raw_line in gz:
                        line = raw_line.decode("utf-8").strip()
                        if line:
                            record = json.loads(line)
                            if _matches_country(record.get("countries_tags"), country):
                                records.append(record)
            return records
        except (requests.exceptions.ChunkedEncodingError, requests.exceptions.ConnectionError, ProtocolError) as e:
            if attempt == max_retries:
                raise
            wait = 2 ** attempt
            logging.warning(f"Connection error on {filename} (attempt {attempt}/{max_retries}), retrying in {wait}s: {e}")
            time.sleep(wait)


def handle(output_file_key, url, num_files=None, last_processed_file=None, country="canada"):
    """Download delta exports, merge them, and upload as parquet to S3.

    Args:
        output_file_key: S3 key where the resulting parquet will be stored.
        url: URL to the delta index.txt file.
        num_files: Number of most recent delta files to process (None = all).
            Only used when last_processed_file is not provided.
        last_processed_file: Filename of the last successfully processed delta file.
            When provided, only files strictly after this one are processed.
    """
    logging.info(f"Fetching delta index from {url}...")

    s3_bucket = os.environ["S3_BUCKET"]
    s3_endpoint = os.environ["S3_ENDPOINT"]
    s3_access_key = os.environ["S3_ACCESS_KEY"]
    s3_secret_key = os.environ["S3_SECRET_KEY"]

    session = requests.Session()
    session.headers.update({"User-Agent": "FoodHealthAdvisor/1.0"})

    # Derive base URL for individual delta files from the index URL
    base_url = url.rsplit("/", 1)[0] + "/"

    filenames = _get_delta_filenames(session, url)
    if not filenames:
        logging.warning("No delta files available.")
        return

    # Sort alphabetically (which is chronological due to UNIX timestamp naming)
    filenames.sort()

    if last_processed_file is not None:
        # Only keep files strictly after the last processed one
        filenames = [f for f in filenames if f > last_processed_file]
        logging.info(f"Resuming after '{last_processed_file}': {len(filenames)} new file(s) to process.")
    elif num_files is not None:
        filenames = filenames[-num_files:]

    if not filenames:
        logging.info("No new delta files to process.")
        return

    logging.info(f"Processing {len(filenames)} delta file(s)...")

    all_records = []
    for filename in filenames:
        records = _download_delta(session, base_url, filename, country)
        all_records.extend(records)
        logging.info(f"  {filename}: {len(records)} {country} records")

    if not all_records:
        logging.warning("No Canadian products found in delta files.")
        return

    logging.info(f"Total Canadian records: {len(all_records)}")

    jsonl = "\n".join(json.dumps(record, ensure_ascii=False) for record in all_records)
    file_bytes = BytesIO(jsonl.encode("utf-8"))

    s3_handler = S3FileHandler(s3_bucket, s3_endpoint, s3_access_key, s3_secret_key)
    s3_handler.upload_from_memory(file_bytes, output_file_key)

    logging.info(f"Delta data uploaded to S3 at '{output_file_key}' ({len(all_records)} records)")
    # Préfixe standardisé pour que le DAG puisse extraire cette valeur et mettre à jour la Variable Airflow
    logging.info(f"LAST_PROCESSED_FILE={filenames[-1]}")
