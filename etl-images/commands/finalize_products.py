import os
import logging
import pandas as pd
from common.s3 import S3FileHandler

logger = logging.getLogger(__name__)

COLUMNS_TO_DROP = ["categories_tags", "ingredients"]


def handle(input_file_key, categorie_principale_input_key, output_file_key):
    """Supprime les colonnes normalisées, merge categorie_principale et produit
    la table products finale sur S3.

    Doit être exécuté après normalize_categories et normalize_ingredients,
    qui produisent leurs tables de référence et de jonction en parallèle.

    Le merge de categorie_principale se fait ici (et non dans normalize_categories)
    pour éviter toute écriture concurrente sur le parquet transformé pendant
    que normalize_ingredients tourne en parallèle.

    Args:
        input_file_key:                 Clé S3 du parquet transformé (silver brut).
        categorie_principale_input_key: Clé S3 du parquet (code, categorie_principale)
                                        produit par normalize_categories.
        output_file_key:                Clé S3 de sortie pour la table products finale.
    """
    s3_bucket     = os.environ["S3_BUCKET"]
    s3_endpoint   = os.environ["S3_ENDPOINT"]
    s3_access_key = os.environ["S3_ACCESS_KEY"]
    s3_secret_key = os.environ["S3_SECRET_KEY"]

    logger.info(f"Finalizing products from {input_file_key}...")

    s3_handler = S3FileHandler(s3_bucket, s3_endpoint, s3_access_key, s3_secret_key)

    raw = s3_handler.download_to_memory(input_file_key)
    df  = pd.read_parquet(raw)

    raw_cp = s3_handler.download_to_memory(categorie_principale_input_key)
    df_cp  = pd.read_parquet(raw_cp)

    # LEFT JOIN : tous les produits sont conservés, categorie_principale est
    # NULL pour ceux qui n'avaient pas de categories_tags exploitables.
    df = df.merge(df_cp, on="code", how="left")
    logger.info(
        f"categorie_principale merged from {categorie_principale_input_key} "
        f"({df['categorie_principale'].notna().sum()}/{len(df)} produits avec FK)"
    )

    cols_present = [c for c in COLUMNS_TO_DROP if c in df.columns]
    df = df.drop(columns=cols_present)

    logger.info(f"Dropped columns: {cols_present}")

    s3_handler.upload_dataframe(df, output_file_key)
    logger.info(f"products uploaded → {output_file_key} ({len(df)} records)")
