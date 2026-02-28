import os
import duckdb
import logging
import tempfile
from common.s3 import S3FileHandler

logging.basicConfig(level=logging.INFO)


def handle(input_file_key, table_name, schema_name):
    logging.info(f"Loading {input_file_key} from S3 to DUCKDB...")

    # Configuration S3
    s3_bucket = os.environ.get("S3_BUCKET")
    s3_endpoint = os.environ.get("S3_ENDPOINT")
    s3_access_key = os.environ.get("S3_ACCESS_KEY")
    s3_secret_key = os.environ.get("S3_SECRET_KEY")

    motherduck_token = os.environ.get("DUCKDB_TOKEN")
    motherduck_db = os.environ.get("DUCKDB_DB")

    # Télécharger le fichier parquet depuis S3 dans un fichier temporaire
    s3_handler = S3FileHandler(s3_bucket, s3_endpoint, s3_access_key, s3_secret_key)
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=True) as tmp:
        s3_handler.download(input_file_key, tmp.name)

        # Insérer dans MotherDuck directement depuis le parquet
        con = duckdb.connect(f"md:{motherduck_db}?motherduck_token={motherduck_token}")
        con.sql(f"CREATE OR REPLACE TABLE {schema_name}.{table_name} AS SELECT * FROM read_parquet('{tmp.name}')")
        con.close()

    logging.info(f"Data inserted into DUCKDB table '{schema_name}.{table_name}'")
