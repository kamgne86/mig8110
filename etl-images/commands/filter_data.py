import os
import logging
import pandas as pd
from common.s3 import S3FileHandler

logger = logging.getLogger(__name__)


def _parse_tags(tags):
    if tags is None:
        return []
    if isinstance(tags, str):
        import ast
        try:
            parsed = ast.literal_eval(tags)
            return parsed if isinstance(parsed, list) else []
        except (ValueError, SyntaxError):
            return []
    try:
        return list(tags)
    except TypeError:
        return []


def _matches_country(tags, country):
    return any(country.lower() in tag.lower() for tag in _parse_tags(tags))


def _matches_lang(lang_value, lang):
    """Check if the lang column matches the given language code."""
    return isinstance(lang_value, str) and lang_value.lower() == lang.lower()


def handle(input_file_key, output_file_key, columns, country=None, lang=None):
    s3_bucket = os.environ["S3_BUCKET"]
    s3_endpoint = os.environ["S3_ENDPOINT"]
    s3_access_key = os.environ["S3_ACCESS_KEY"]
    s3_secret_key = os.environ["S3_SECRET_KEY"]

    logger.info(f"Filtering columns from {input_file_key}...")

    column_list = [col.strip() for col in columns.split(",")]

    s3_handler = S3FileHandler(s3_bucket, s3_endpoint, s3_access_key, s3_secret_key)

    parquet_bytes = s3_handler.download_to_memory(input_file_key)
    df = pd.read_parquet(parquet_bytes, columns=column_list)

    if country is not None:
        df = df[df["countries_tags"].apply(lambda tags: _matches_country(tags, country))]
        logger.info(f"Country filter '{country}': {len(df)} records remaining.")

    if lang is not None:
        df = df[df["lang"].apply(lambda l: _matches_lang(l, lang))]
        logger.info(f"Lang filter '{lang}': {len(df)} records remaining.")

    s3_handler.upload_dataframe(df, output_file_key)

    logger.info(f"Data uploaded to S3: {output_file_key} ({len(df)} records, {len(column_list)} columns)")
