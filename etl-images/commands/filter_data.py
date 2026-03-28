import os
import logging
import pandas as pd
from common.s3 import S3FileHandler

logger = logging.getLogger(__name__)


def handle(input_file_key, output_file_key, columns):
    s3_bucket = os.environ["S3_BUCKET"]
    s3_endpoint = os.environ["S3_ENDPOINT"]
    s3_access_key = os.environ["S3_ACCESS_KEY"]
    s3_secret_key = os.environ["S3_SECRET_KEY"]

    logger.info(f"Filtering columns from {input_file_key}...")

    column_list = [col.strip() for col in columns.split(",")]

    s3_handler = S3FileHandler(s3_bucket, s3_endpoint, s3_access_key, s3_secret_key)

    parquet_bytes = s3_handler.download_to_memory(input_file_key)
    df = pd.read_parquet(parquet_bytes, columns=column_list)

    s3_handler.upload_dataframe(df, output_file_key)

    logger.info(f"Data uploaded to S3: {output_file_key} ({len(df)} records, {len(column_list)} columns)")
