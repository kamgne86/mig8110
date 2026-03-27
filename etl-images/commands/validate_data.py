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

    raw = s3_handler.download_to_memory(input_file_key)
    reader = pd.read_json(raw, lines=True, chunksize=500) if input_file_key.endswith(".jsonl") else [pd.read_parquet(raw)]

    chunks_valid, chunks_invalid = [], []
    for chunk in reader:
        mask = pd.Series(True, index=chunk.index)
        for rule in VALIDATION_RULES:
            mask &= rule(chunk)
        chunks_valid.append(chunk[mask])
        chunks_invalid.append(chunk[~mask])

    df_valid   = pd.concat(chunks_valid,   ignore_index=True)
    df_invalid = pd.concat(chunks_invalid, ignore_index=True)

    logging.info(f"Valid: {len(df_valid)} records, Invalid: {len(df_invalid)} records")

    _upload_df(s3_handler, df_valid, output_file_key)
    logging.info(f"Valid data uploaded to S3: {output_file_key}")

    _upload_df(s3_handler, df_invalid, invalid_file_key)
    logging.info(f"Invalid data uploaded to S3: {invalid_file_key}")


def _upload_df(s3_handler, df, file_key):
    buf = BytesIO()
    if file_key.endswith(".jsonl"):
        buf.write(df.to_json(orient="records", lines=True, force_ascii=False).encode("utf-8"))
    else:
        df.to_parquet(buf, index=False)
    buf.seek(0)
    s3_handler.upload_from_memory(buf, file_key)
