import os
import logging
import pandas as pd
from io import BytesIO
from common.s3 import S3FileHandler
from config.target_columns import TARGET_COLUMNS

logging.basicConfig(level=logging.INFO)

BASE_IMAGE_URL = "https://images.openfoodfacts.org/images/products"

NUTRIMENTS = [
    ("energy-kcal",    "energy_kcal_100g"),
    ("fat",            "fat_100g"),
    ("saturated-fat",  "saturated_fat_100g"),
    ("trans-fat",      "trans_fat_100g"),
    ("cholesterol",    "cholesterol_100g"),
    ("sodium",         "sodium_100g"),
    ("salt",           "salt_100g"),
    ("carbohydrates",  "carbohydrates_100g"),
    ("fiber",          "fiber_100g"),
    ("sugars",         "sugars_100g"),
    ("proteins",       "proteins_100g"),
    ("calcium",        "calcium_100g"),
    ("iron",           "iron_100g"),
    ("potassium",      "potassium_100g"),
]

IMAGE_KEYS = [
    ("front_en",       "front_url"),
    ("ingredients_en", "ingredients_url"),
    ("nutrition_en",   "nutrition_url"),
    ("packaging_en",   "packaging_url"),
]


def _extract_product_name(product_name_list):
    if product_name_list is None:
        return None
    try:
        for item in product_name_list:
            if item['lang'] == 'main':
                return item['text']
    except TypeError:
        return None
    return None


def _build_code_path(code):
    code_padded = str(code).zfill(13)
    return f"{code_padded[:3]}/{code_padded[3:6]}/{code_padded[6:9]}/{code_padded[9:]}"


def _extract_image_url(images_list, code, image_key):
    if images_list is None:
        return None
    try:
        for item in images_list:
            if item['key'] == image_key:
                rev = item['rev']
                if rev is not None:
                    return f"{BASE_IMAGE_URL}/{_build_code_path(code)}/{image_key}.{int(rev)}.400.jpg"
    except TypeError:
        return None
    return None


def _extract_nutriment(nutriments_list, nutriment_name):
    if nutriments_list is None:
        return None
    try:
        for item in nutriments_list:
            if item['name'] == nutriment_name:
                return item['100g']
    except TypeError:
        return None
    return None


def handle(input_file_key, output_file_key):
    logging.info(f"Transforming data from {input_file_key}...")

    s3_bucket = os.environ["S3_BUCKET"]
    s3_endpoint = os.environ["S3_ENDPOINT"]
    s3_access_key = os.environ["S3_ACCESS_KEY"]
    s3_secret_key = os.environ["S3_SECRET_KEY"]

    s3_handler = S3FileHandler(s3_bucket, s3_endpoint, s3_access_key, s3_secret_key)

    parquet_bytes = s3_handler.download_to_memory(input_file_key)
    df = pd.read_parquet(parquet_bytes)

    # T2 — Transformations

    # product_name: extraire le texte "main" depuis la liste
    df['product_name'] = df['product_name'].apply(_extract_product_name)

    # image URLs: construire depuis code + liste images
    for image_key, col_name in IMAGE_KEYS:
        df[col_name] = [
            _extract_image_url(images, code, image_key)
            for images, code in zip(df['images'], df['code'])
        ]

    # nutriments: extraire les valeurs depuis la liste
    for nutriment_name, col_name in NUTRIMENTS:
        df[col_name] = df['nutriments'].apply(
            lambda lst, n=nutriment_name: _extract_nutriment(lst, n)
        )

    # nutriscore_grade et ecoscore_grade: mettre à NULL les valeurs non reconnues
    # afin de conserver les enregistrements tout en signalant l'absence de score
    df['nutriscore_grade'] = df['nutriscore_grade'].where(
        df['nutriscore_grade'].isin(['a', 'b', 'c', 'd', 'e']), None
    )
    df['ecoscore_grade'] = df['ecoscore_grade'].where(
        df['ecoscore_grade'].isin(['a-plus', 'a', 'b', 'c', 'd', 'e', 'f']), None
    )

    # Projection sur le schéma Silver — garantit la compatibilité avec le delta load
    for col in TARGET_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[TARGET_COLUMNS]

    # Upload f3 (transformé) → output_file_key
    transformed_bytes = BytesIO()
    df.to_parquet(transformed_bytes, index=False)
    transformed_bytes.seek(0)
    s3_handler.upload_from_memory(transformed_bytes, output_file_key)

    logging.info(f"Transformed data uploaded to S3: {output_file_key} ({len(df)} records)")
