import os
import logging
import pandas as pd
from common.s3 import S3FileHandler

logger = logging.getLogger(__name__)


def _resolve_columns(df, columns):
    """Resolve column names with optional fallback syntax (target|fallback).

    For each entry:
    - "code"                   → keep column as-is
    - "ecoscore_grade|environmental_score_grade" → use ecoscore_grade if present,
      otherwise use environmental_score_grade and rename it to ecoscore_grade
    - If neither name exists, the column is skipped (filled with None by caller)

    Returns a dict: {target_name: actual_column_name_in_df}
    """
    resolved = {}
    for entry in columns:
        parts = [p.strip() for p in entry.split("|")]
        target = parts[0]
        candidates = parts

        for candidate in candidates:
            if candidate in df.columns:
                resolved[target] = candidate
                break

    return resolved


def handle(input_file_key, output_file_key, columns):
    """Read a delta parquet from S3, select requested columns with fallback support,
    and upload the result as parquet.

    Columns missing from the file are added with None values to guarantee a
    consistent schema regardless of which delta file is being processed.

    Args:
        columns: Comma-separated column specs. Each spec can use pipe syntax for
                 fallback: "ecoscore_grade|environmental_score_grade" means use
                 ecoscore_grade if present, else environmental_score_grade (renamed).
    """
    s3_bucket = os.environ["S3_BUCKET"]
    s3_endpoint = os.environ["S3_ENDPOINT"]
    s3_access_key = os.environ["S3_ACCESS_KEY"]
    s3_secret_key = os.environ["S3_SECRET_KEY"]

    column_specs = [col.strip() for col in columns.split(",")]

    s3_handler = S3FileHandler(s3_bucket, s3_endpoint, s3_access_key, s3_secret_key)

    parquet_bytes = s3_handler.download_to_memory(input_file_key)
    df = pd.read_parquet(parquet_bytes)

    resolved = _resolve_columns(df, column_specs)

    # Build output DataFrame: rename fallback columns, fill missing ones with None
    result = pd.DataFrame()
    for spec in column_specs:
        target = spec.split("|")[0].strip()
        if target in resolved:
            result[target] = df[resolved[target]]
        else:
            logger.warning(f"Column '{target}' not found in {input_file_key}, filling with None.")
            result[target] = None

    s3_handler.upload_dataframe(result, output_file_key)

    logger.info(f"Filtered delta uploaded to {output_file_key} ({len(result)} records, {len(column_specs)} columns).")
