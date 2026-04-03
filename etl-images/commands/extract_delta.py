import os
import gzip
import json
import logging
import tempfile
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from urllib.parse import urljoin
from common.s3 import S3FileHandler

logger = logging.getLogger(__name__)

CHUNK_SIZE = 50 * 1024 * 1024  # 50 MB
BATCH_SIZE = 500


def _matches_country(tags, country):
    """Check if any of the countries_tags contains the given country string."""
    return isinstance(tags, list) and any(country.lower() in tag.lower() for tag in tags)


def _download_and_filter(session, url, country):
    """Download a gzipped delta file in 50 MB chunks, decompress, and filter by country.

    Range requests ensure that a retry only re-downloads the failed chunk
    rather than restarting the entire file from the beginning.

    Records are written to a parquet file in batches of BATCH_SIZE to keep
    memory usage constant regardless of the file size.
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

    out_tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    out_path = out_tmp.name
    out_tmp.close()

    def _flush_batch(batches, writer):
        df_batch = pd.DataFrame(batches)
        for col in df_batch.columns:
            if df_batch[col].dtype == object:
                df_batch[col] = df_batch[col].apply(
                    lambda x: None if (x is None or (isinstance(x, float) and x != x))
                    else str(x)
                )
        table = pa.Table.from_pandas(df_batch, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(out_path, table.schema)
        writer.write_table(table)
        return writer

    try:
        batches = []
        writer = None
        total_records = 0

        with gzip.open(tmp_path, "rt", encoding="utf-8") as gz:
            for line in gz:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                if not _matches_country(record.get("countries_tags"), country):
                    continue

                for key in record:
                    if isinstance(record[key], (list, dict)):
                        record[key] = json.dumps(record[key], ensure_ascii=False)

                batches.append(record)

                if len(batches) >= BATCH_SIZE:
                    writer = _flush_batch(batches, writer)
                    total_records += len(batches)
                    batches.clear()

            if batches:
                writer = _flush_batch(batches, writer)
                total_records += len(batches)

        if writer:
            writer.close()

        logger.info(f"Found {total_records} {country} records.")
        return out_path, total_records
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

    parquet_path, total_records = _download_and_filter(session, url, country)

    if total_records == 0:
        logger.warning(f"No {country} records found in {filename}, uploading empty parquet.")

    s3_handler = S3FileHandler(s3_bucket, s3_endpoint, s3_access_key, s3_secret_key)
    try:
        with open(parquet_path, "rb") as f:
            s3_handler.upload_from_memory(f, output_file_key)
    finally:
        os.unlink(parquet_path)

    logger.info(f"Uploaded {output_file_key} ({total_records} records).")
