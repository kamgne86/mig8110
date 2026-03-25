import os
import gzip
import json
import logging
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


def _is_canadian(tags):
    """Check if a product's countries_tags contains Canada."""
    return isinstance(tags, list) and "en:canada" in tags


def _download_delta(session, base_url, filename):
    """Stream a gzipped JSONL delta file and return only Canadian product records."""
    url = urljoin(base_url, filename)
    logging.info(f"Downloading delta file: {url}")

    records = []
    with session.get(url, timeout=120, stream=True) as response:
        response.raise_for_status()
        with gzip.GzipFile(fileobj=response.raw) as gz:
            for raw_line in gz:
                line = raw_line.decode("utf-8").strip()
                if line:
                    record = json.loads(line)
                    if _is_canadian(record.get("countries_tags")):
                        records.append(record)
    return records


def handle(output_file_key, url, num_files=None):
    """Download delta exports, merge them, and upload as parquet to S3.

    Args:
        output_file_key: S3 key where the resulting parquet will be stored.
        url: URL to the delta index.txt file.
        num_files: Number of most recent delta files to process (None = all).
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

    if num_files is not None:
        filenames = filenames[-num_files:]

    logging.info(f"Processing {len(filenames)} delta file(s)...")

    all_records = []
    for filename in filenames:
        records = _download_delta(session, base_url, filename)
        all_records.extend(records)
        logging.info(f"  {filename}: {len(records)} Canadian records")

    if not all_records:
        logging.warning("No Canadian products found in delta files.")
        return

    logging.info(f"Total Canadian records: {len(all_records)}")

    jsonl = "\n".join(json.dumps(record) for record in all_records)
    file_bytes = BytesIO(jsonl.encode("utf-8"))

    s3_handler = S3FileHandler(s3_bucket, s3_endpoint, s3_access_key, s3_secret_key)
    s3_handler.upload_from_memory(file_bytes, output_file_key)

    logging.info(f"Delta data uploaded to S3 at '{output_file_key}'")
