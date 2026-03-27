import os
import logging
import pandas as pd
from io import BytesIO
from common.s3 import S3FileHandler
from config.validation_rules import VALIDATION_RULES

logging.basicConfig(level=logging.INFO)


def handle(input_file_key, output_file_key, invalid_file_key):
    logging.info(f"Validating data from {input_file_key}...")

    s3_bucket = os.environ["S3_BUCKET"]
    s3_endpoint = os.environ["S3_ENDPOINT"]
    s3_access_key = os.environ["S3_ACCESS_KEY"]
    s3_secret_key = os.environ["S3_SECRET_KEY"]

    s3_handler = S3FileHandler(s3_bucket, s3_endpoint, s3_access_key, s3_secret_key)

    parquet_bytes = s3_handler.download_to_memory(input_file_key)
    df = pd.read_parquet(parquet_bytes)

    # T1 — Appliquer toutes les règles de validation
    mask_valid = pd.Series([True] * len(df), index=df.index)
    for rule in VALIDATION_RULES:
        mask_valid &= rule(df)

    df_valid = df[mask_valid].copy()
    df_invalid = df[~mask_valid].copy()

    logging.info(f"Valid: {len(df_valid)} records, Invalid: {len(df_invalid)} records")

    # Upload f1 (valides) → output_file_key
    valid_bytes = BytesIO()
    df_valid.to_parquet(valid_bytes, index=False)
    valid_bytes.seek(0)
    s3_handler.upload_from_memory(valid_bytes, output_file_key)
    logging.info(f"Valid data uploaded to S3: {output_file_key}")

    # Upload f2 (invalides) → invalid_file_key
    invalid_bytes = BytesIO()
    df_invalid.to_parquet(invalid_bytes, index=False)
    invalid_bytes.seek(0)
    s3_handler.upload_from_memory(invalid_bytes, invalid_file_key)
    logging.info(f"Invalid data uploaded to S3: {invalid_file_key}")
