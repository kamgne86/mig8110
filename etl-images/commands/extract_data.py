import os
import logging
import zipfile
import requests
from io import BytesIO
from common.s3 import S3FileHandler

logger = logging.getLogger(__name__)


def handle(output_file_key, url):
    s3_bucket = os.environ["S3_BUCKET"]
    s3_endpoint = os.environ["S3_ENDPOINT"]
    s3_access_key = os.environ["S3_ACCESS_KEY"]
    s3_secret_key = os.environ["S3_SECRET_KEY"]

    logger.info(f"Retrieving data from URL {url}...")

    # Télécharger et traiter en mémoire
    session = requests.Session()
    session.headers.update({"User-Agent": "FoodHealthAdvisor/1.0"})
    response = session.get(url, timeout=(10, 60))
    response.raise_for_status()
    
    # Dézipper et lire les bytes du parquet directement
    with zipfile.ZipFile(BytesIO(response.content)) as zip_ref:
        parquet_files = [f for f in zip_ref.namelist() if f.endswith(".parquet")]
        if not parquet_files:
            raise ValueError(f"No parquet file found in zip. Contents: {zip_ref.namelist()}")
        parquet_file = parquet_files[0]
        parquet_bytes = BytesIO(zip_ref.read(parquet_file))
    
    s3_handler = S3FileHandler(s3_bucket, s3_endpoint, s3_access_key, s3_secret_key)
    s3_handler.upload_from_memory(parquet_bytes, output_file_key)
    
    logger.info(f"Data uploaded to S3 successfully")
