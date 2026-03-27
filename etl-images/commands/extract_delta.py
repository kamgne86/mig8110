import os
import gzip
import json
import logging
import tempfile
import requests
from io import BytesIO
from urllib.parse import urljoin
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


CHUNK_SIZE = 50 * 1024 * 1024  # 50 MB


def _download_delta(session, base_url, filename, country):
    """Télécharge un fichier delta gzippé par chunks (Range requests) vers un fichier
    temporaire, puis décompresse et filtre par pays.

    Le retry s'applique par chunk de 50MB — un fichier de 1.3GB nécessite ~27 chunks
    au lieu de recommencer depuis le début à chaque erreur.
    """
    url = urljoin(base_url, filename)

    head = session.head(url, timeout=30)
    head.raise_for_status()
    total_size = int(head.headers["Content-Length"])
    logging.info(f"Downloading {filename} ({total_size / 1024 / 1024:.0f} MB) in {CHUNK_SIZE // 1024 // 1024} MB chunks...")

    with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as tmp:
        tmp_path = tmp.name
        downloaded = 0
        while downloaded < total_size:
            end = min(downloaded + CHUNK_SIZE - 1, total_size - 1)
            resp = session.get(url, headers={"Range": f"bytes={downloaded}-{end}"}, timeout=120)
            resp.raise_for_status()
            tmp.write(resp.content)
            downloaded += len(resp.content)
            logging.info(f"  {downloaded / total_size * 100:.1f}% ({downloaded // 1024 // 1024} MB / {total_size // 1024 // 1024} MB)")

    try:
        records = []
        with gzip.open(tmp_path, "rt", encoding="utf-8") as gz:
            for line in gz:
                line = line.strip()
                if line:
                    record = json.loads(line)
                    if _matches_country(record.get("countries_tags"), country):
                        records.append(record)
        return records
    finally:
        os.unlink(tmp_path)


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

    last_filename = filenames[-1]
    xcom_dir = "/airflow/xcom"
    try:
        os.makedirs(xcom_dir, exist_ok=True)
        with open(f"{xcom_dir}/return.json", "w") as f:
            json.dump({"last_processed_file": last_filename}, f)
        logging.info(f"XCom written: last_processed_file={last_filename}")
    except OSError:
        # Hors Kubernetes (tests locaux), le répertoire n'existe pas
        logging.info(f"LAST_PROCESSED_FILE={last_filename}")
