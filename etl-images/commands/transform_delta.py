import os
import json
import logging
import pandas as pd
from io import BytesIO
from common.s3 import S3FileHandler
from config.target_columns import TARGET_COLUMNS

logging.basicConfig(level=logging.INFO)

BASE_IMAGE_URL = "https://images.openfoodfacts.org/images/products"

NUTRIMENTS = [
    ("energy-kcal",   "energy_kcal_100g"),
    ("fat",           "fat_100g"),
    ("saturated-fat", "saturated_fat_100g"),
    ("trans-fat",     "trans_fat_100g"),
    ("cholesterol",   "cholesterol_100g"),
    ("sodium",        "sodium_100g"),
    ("salt",          "salt_100g"),
    ("carbohydrates", "carbohydrates_100g"),
    ("fiber",         "fiber_100g"),
    ("sugars",        "sugars_100g"),
    ("proteins",      "proteins_100g"),
    ("calcium",       "calcium_100g"),
    ("iron",          "iron_100g"),
    ("potassium",     "potassium_100g"),
]

# Mapping: clé dans images.selected → nom de fichier image
IMAGE_KEYS = [
    ("front",        "front_en",        "front_url"),
    ("ingredients",  "ingredients_en",  "ingredients_url"),
    ("nutrition",    "nutrition_en",    "nutrition_url"),
    ("packaging",    "packaging_en",    "packaging_url"),
]



def _build_code_path(code):
    code_padded = str(code).zfill(13)
    return f"{code_padded[:3]}/{code_padded[3:6]}/{code_padded[6:9]}/{code_padded[9:]}"


def _extract_image_url(images, code, selected_key, image_file_key):
    """Extrait l'URL d'une image depuis la structure images.selected du format delta."""
    if images is None:
        return None
    try:
        en_data = images['selected'][selected_key]['en']
        rev_raw = en_data['rev']
        if rev_raw is None:
            return None
        # Le rev peut être entouré de guillemets dans le JSON d'origine (ex: '"7"')
        rev = str(rev_raw).strip('"')
        return f"{BASE_IMAGE_URL}/{_build_code_path(code)}/{image_file_key}.{rev}.400.jpg"
    except (TypeError, KeyError):
        return None


def _parse_json(value):
    """Parse a JSON string back to a Python object.

    Columns serialized in extract_delta (lists, dicts → JSON strings) must be
    parsed back before transformations and before writing to parquet, so that
    the output schema matches the target table (e.g. VARCHAR[] for tag columns).
    """
    if value is None or (isinstance(value, float) and value != value):
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return None
    return value


def _extract_nutriment(nutrition, nutriment_name):
    """Extrait une valeur nutritionnelle depuis la structure nutrition.aggregated_set.nutrients."""
    if nutrition is None:
        return None
    try:
        value = nutrition['aggregated_set']['nutrients'][nutriment_name]['value']
        return round(value, 2) if value is not None else None
    except (TypeError, KeyError):
        return None


def handle(input_file_key, output_file_key):
    logging.info(f"Transforming delta data from {input_file_key}...")

    s3_bucket = os.environ["S3_BUCKET"]
    s3_endpoint = os.environ["S3_ENDPOINT"]
    s3_access_key = os.environ["S3_ACCESS_KEY"]
    s3_secret_key = os.environ["S3_SECRET_KEY"]

    s3_handler = S3FileHandler(s3_bucket, s3_endpoint, s3_access_key, s3_secret_key)

    raw = s3_handler.download_to_memory(input_file_key)
    if input_file_key.endswith(".jsonl"):
        df = pd.read_json(raw, lines=True)
    else:
        df = pd.read_parquet(raw)

    # T2 — Transformations (format delta)

    # Les colonnes complexes (listes, dicts) ont été sérialisées en JSON strings dans extract_delta.
    # On les parse ici avant les transformations pour restaurer les types attendus :
    # - images, nutriments : dicts nécessaires pour l'extraction
    # - categories_tags, countries_tags, ingredients_tags : listes → VARCHAR[] dans MotherDuck
    for col in ['images', 'nutrition', 'categories_tags', 'countries_tags', 'ingredients_tags']:
        if col in df.columns:
            df[col] = df[col].apply(_parse_json)

    # product_name: déjà un VARCHAR dans le delta, aucune extraction nécessaire

    # image URLs: extraire depuis images.selected.{type}.en.rev
    for selected_key, image_file_key, col_name in IMAGE_KEYS:
        df[col_name] = [
            _extract_image_url(images, code, selected_key, image_file_key)
            for images, code in zip(df['images'], df['code'])
        ]

    # nutrition: extraire depuis nutrition.aggregated_set.nutrients.{name}.value
    for nutriment_name, col_name in NUTRIMENTS:
        df[col_name] = df['nutrition'].apply(
            lambda n, name=nutriment_name: _extract_nutriment(n, name)
        )

    # nutriscore_grade et ecoscore_grade: normaliser les valeurs non reconnues à NULL
    # Ces colonnes peuvent être absentes de certains enregistrements delta
    if 'nutriscore_grade' in df.columns:
        df['nutriscore_grade'] = df['nutriscore_grade'].where(
            df['nutriscore_grade'].isin(['a', 'b', 'c', 'd', 'e']), None
        )
    if 'ecoscore_grade' in df.columns:
        df['ecoscore_grade'] = df['ecoscore_grade'].where(
            df['ecoscore_grade'].isin(['a-plus', 'a', 'b', 'c', 'd', 'e', 'f']), None
        )

    # Projection sur le schéma Silver — garantit la compatibilité avec l'initial load
    for col in TARGET_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[TARGET_COLUMNS]

    # Upload f3 (transformé) → output_file_key
    transformed_bytes = BytesIO()
    df.to_parquet(transformed_bytes, index=False)
    transformed_bytes.seek(0)
    s3_handler.upload_from_memory(transformed_bytes, output_file_key)

    logging.info(f"Transformed delta data uploaded to S3: {output_file_key} ({len(df)} records)")
