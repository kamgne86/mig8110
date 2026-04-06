import os
import logging
import pandas as pd
from common.s3 import S3FileHandler
from common.monitoring import record_run
from config.validation_rules_delta import VALIDATION_RULES

logger = logging.getLogger(__name__)


def handle(input_file_key, output_file_key, invalid_file_key, schema_name, table_name):
    s3_bucket = os.environ["S3_BUCKET"]
    s3_endpoint = os.environ["S3_ENDPOINT"]
    s3_access_key = os.environ["S3_ACCESS_KEY"]
    s3_secret_key = os.environ["S3_SECRET_KEY"]

    logger.info(f"Validating delta from {input_file_key}...")

    s3_handler = S3FileHandler(s3_bucket, s3_endpoint, s3_access_key, s3_secret_key)

    raw = s3_handler.download_to_memory(input_file_key)
    df = pd.read_parquet(raw)

    mask = pd.Series(True, index=df.index)
    for name, rule in VALIDATION_RULES:
        rule_mask = rule(df)
        failed = (~rule_mask).sum()
        if failed:
            logger.info(f"  Rule '{name}': {failed} records failed")
        mask &= rule_mask

    df_valid   = df[mask].reset_index(drop=True)
    df_invalid = df[~mask].reset_index(drop=True)

    logger.info(f"Valid: {len(df_valid)} records, Invalid: {len(df_invalid)} records")

    s3_handler.upload_dataframe(df_valid, output_file_key)
    logger.info(f"Data uploaded to S3: {output_file_key}")

    s3_handler.upload_dataframe(df_invalid, invalid_file_key)
    logger.info(f"Data uploaded to S3: {invalid_file_key}")

    stem = input_file_key.split("/")[-1].replace(".parquet", "")
    record_run(
        command=f"validate_delta/{stem}",
        records_in=len(df_valid) + len(df_invalid),
        records_out=len(df_valid),
        records_rejected=len(df_invalid),
        monitoring_schema=schema_name,
        monitoring_table=table_name,
    )
