import os
import logging
import pandas as pd
from common.s3 import S3FileHandler

logger = logging.getLogger(__name__)

COLUMNS_TO_DROP = ["categories_tags", "ingredients_tags"]


def handle(input_file_key, output_file_key):
    """Supprime les colonnes normalisées et produit la table products finale sur S3.

    Doit être exécuté après normalize_categories et normalize_ingredients,
    qui produisent leurs tables de référence et de jonction en parallèle.

    Args:
        input_file_key:  Clé S3 du parquet transformé (silver brut).
        output_file_key: Clé S3 de sortie pour la table products finale.
    """
    s3_bucket     = os.environ["S3_BUCKET"]
    s3_endpoint   = os.environ["S3_ENDPOINT"]
    s3_access_key = os.environ["S3_ACCESS_KEY"]
    s3_secret_key = os.environ["S3_SECRET_KEY"]

    logger.info(f"Finalizing products from {input_file_key}...")

    s3_handler = S3FileHandler(s3_bucket, s3_endpoint, s3_access_key, s3_secret_key)

    raw = s3_handler.download_to_memory(input_file_key)
    df  = pd.read_parquet(raw)

    cols_present = [c for c in COLUMNS_TO_DROP if c in df.columns]
    df = df.drop(columns=cols_present)

    logger.info(f"Dropped columns: {cols_present}")

    s3_handler.upload_dataframe(df, output_file_key)
    logger.info(f"products uploaded → {output_file_key} ({len(df)} records)")
