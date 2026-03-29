import os
import gzip
import json
import logging
import tempfile
import pandas as pd
import requests
from urllib.parse import urljoin
from common.s3 import S3FileHandler

logger = logging.getLogger(__name__)

CHUNK_SIZE = 50 * 1024 * 1024  # 50 MB


def _matches_country(tags, country):
    """Check if any of the countries_tags contains the given country string."""
    return isinstance(tags, list) and any(country.lower() in tag.lower() for tag in tags)


def _download_and_filter(session, url, country):
    """Download a gzipped delta file in 50 MB chunks, decompress, and filter by country.

    Range requests ensure that a retry only re-downloads the failed chunk
    rather than restarting the entire file from the beginning.
    """
    head = session.head(url, timeout=30)
    head.raise_for_status()
    total_size = int(head.headers["Content-Length"])
    logger.info(f"Downloading {url} ({total_size / 1024 / 1024:.0f} MB) in {CHUNK_SIZE // 1024 // 1024} MB chunks...")

    with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as tmp:
        tmp_path = tmp.name
        downloaded = 0
        while downloaded < total_size:
            end = min(downloaded + CHUNK_SIZE - 1, total_size - 1)
            resp = session.get(url, headers={"Range": f"bytes={downloaded}-{end}"}, timeout=120)
            resp.raise_for_status()
            tmp.write(resp.content)
            downloaded += len(resp.content)
            logger.info(f"  {downloaded / total_size * 100:.1f}% ({downloaded // 1024 // 1024} MB / {total_size // 1024 // 1024} MB)")

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


def handle(filename, output_file_key, base_url, country="canada"):
    """Download a single delta file, filter by country, and upload as parquet to S3.

    Complex columns (lists, dicts) are serialized to JSON strings so that
    PyArrow can write a clean parquet file without mixed-type conflicts.

    Args:
        filename: Name of the delta file to process (e.g. openfoodfacts_products_xxx.json.gz).
        output_file_key: S3 key where the resulting parquet will be stored.
        base_url: Base URL of the delta directory (e.g. https://static.openfoodfacts.org/data/delta/).
        country: Country to filter on (substring match against countries_tags, default: canada).
    """
    s3_bucket = os.environ["S3_BUCKET"]
    s3_endpoint = os.environ["S3_ENDPOINT"]
    s3_access_key = os.environ["S3_ACCESS_KEY"]
    s3_secret_key = os.environ["S3_SECRET_KEY"]

    session = requests.Session()
    session.headers.update({"User-Agent": "FoodHealthAdvisor/1.0"})

    url = urljoin(base_url, filename)
    logger.info(f"Processing delta file: {filename}")

    records = _download_and_filter(session, url, country)

    if not records:
        logger.warning(f"No {country} records found in {filename}, uploading empty parquet.")

    logger.info(f"Found {len(records)} {country} records in {filename}.")

    df = pd.DataFrame(records)

    # Serialize object columns to strings for parquet compatibility.
    # Delta files contain mixed-type columns (e.g. int/str in the same column)
    # that PyArrow cannot infer — converting to string avoids type conflicts.
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(
                lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else (None if (x is None or (isinstance(x, float) and x != x)) else str(x))
            )

    s3_handler = S3FileHandler(s3_bucket, s3_endpoint, s3_access_key, s3_secret_key)
    s3_handler.upload_dataframe(df, output_file_key)

    logger.info(f"Uploaded {output_file_key} ({len(df)} records).")
