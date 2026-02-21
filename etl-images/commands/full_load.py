import os
import logging
import zipfile
import requests
import pandas as pd
from io import BytesIO
from common.s3 import S3FileHandler

logging.basicConfig(level=logging.INFO)


def handle(output_file_key, url):
    logging.info(f"Retrieving data from URL {url}...")

    s3_bucket = os.environ["S3_BUCKET"]
    s3_endpoint = os.environ["S3_ENDPOINT"]
    s3_access_key = os.environ["S3_ACCESS_KEY"]
    s3_secret_key = os.environ["S3_SECRET_KEY"]

    # Télécharger et traiter en mémoire
    response = requests.get(url)
    response.raise_for_status()
    
    # Dézipper et charger le parquet en mémoire
    with zipfile.ZipFile(BytesIO(response.content)) as zip_ref:
        parquet_file = [f for f in zip_ref.namelist() if f.endswith(".parquet")][0]
        df = pd.read_parquet(zip_ref.open(parquet_file))
    
    # Uploader directement sur S3
    parquet_bytes = BytesIO()
    df.to_parquet(parquet_bytes, index=False)
    parquet_bytes.seek(0)
    
    s3_handler = S3FileHandler(s3_bucket, s3_endpoint, s3_access_key, s3_secret_key)
    s3_handler.upload_from_memory(parquet_bytes, output_file_key)
    
    logging.info(f"Data uploaded to S3 successfully")
