import os
import logging
import pandas as pd
from sqlalchemy import create_engine
from common.s3 import S3FileHandler

logging.basicConfig(level=logging.INFO)


def handle(input_file_key, table_name, if_exists='append', columns=None):
    logging.info(f"Loading {input_file_key} from S3 to PostgreSQL...")
    
    # Configuration S3
    s3_bucket = os.environ.get("S3_BUCKET")
    s3_endpoint = os.environ.get("S3_ENDPOINT")
    s3_access_key = os.environ.get("S3_ACCESS_KEY")
    s3_secret_key = os.environ.get("S3_SECRET_KEY")
    
    # Configuration PostgreSQL
    pg_host = os.environ.get("PG_HOST")
    pg_port = os.environ.get("PG_PORT", "5432")
    pg_database = os.environ.get("PG_DATABASE")
    pg_user = os.environ.get("PG_USER")
    pg_password = os.environ.get("PG_PASSWORD")
    
    # Télécharger le fichier parquet depuis S3
    s3_handler = S3FileHandler(s3_bucket, s3_endpoint, s3_access_key, s3_secret_key)
    file_obj = s3_handler.download_to_memory(input_file_key)
    
    # Charger le parquet en DataFrame
    df = pd.read_parquet(file_obj)
    if columns:
        df = df[[col.strip() for col in columns.split(',')]]
    logging.info(f"Data loaded: {len(df)} rows, {len(df.columns)} columns")
    
    # Insérer dans PostgreSQL
    connection_string = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}?sslmode=require"
    engine = create_engine(connection_string)
    df.to_sql(table_name, engine, if_exists=if_exists, index=False)
    
    logging.info(f"Data inserted into table '{table_name}'")
