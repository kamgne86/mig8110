import os
import duckdb
import logging
import tempfile
from common.s3 import S3FileHandler

logger = logging.getLogger(__name__)


def handle(input_file_key, table_name, schema_name):
    s3_bucket = os.environ["S3_BUCKET"]
    s3_endpoint = os.environ["S3_ENDPOINT"]
    s3_access_key = os.environ["S3_ACCESS_KEY"]
    s3_secret_key = os.environ["S3_SECRET_KEY"]

    motherduck_token = os.environ["DUCKDB_TOKEN"]
    motherduck_db = os.environ["DUCKDB_DB"]

    logger.info(f"Loading {input_file_key} from S3 to DuckDB...")

    s3_handler = S3FileHandler(s3_bucket, s3_endpoint, s3_access_key, s3_secret_key)
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=True) as tmp:
        s3_handler.download(input_file_key, tmp.name)

        con = duckdb.connect(f"md:{motherduck_db}?motherduck_token={motherduck_token}")
        # CREATE OR REPLACE recrée la table à chaque exécution (chargement initial).
        # Pour le chargement incrémental (delta), une commande dédiée gère l'append.
        con.sql(f"CREATE OR REPLACE TABLE {schema_name}.{table_name} AS SELECT * FROM read_parquet('{tmp.name}')")
        con.close()

    logger.info(f"Data loaded into DuckDB table '{schema_name}.{table_name}'")
