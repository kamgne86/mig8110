import os
import duckdb
import logging
import tempfile
from common.s3 import S3FileHandler

logger = logging.getLogger(__name__)


def handle(input_file_key, table_name, schema_name):
    """Download a transformed delta parquet from S3 and upsert it into MotherDuck.

    Uses a DELETE + INSERT pattern keyed on `code`:
    - Existing rows with matching codes are replaced (updated).
    - New rows are inserted.

    Args:
        input_file_key: S3 key of the transformed parquet to load.
        table_name: Target table name in MotherDuck.
        schema_name: Target schema name in MotherDuck.
    """
    s3_bucket = os.environ["S3_BUCKET"]
    s3_endpoint = os.environ["S3_ENDPOINT"]
    s3_access_key = os.environ["S3_ACCESS_KEY"]
    s3_secret_key = os.environ["S3_SECRET_KEY"]

    motherduck_token = os.environ["DUCKDB_TOKEN"]
    motherduck_db = os.environ["DUCKDB_DB"]

    logger.info(f"Loading {input_file_key} into {schema_name}.{table_name}...")

    s3_handler = S3FileHandler(s3_bucket, s3_endpoint, s3_access_key, s3_secret_key)
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=True) as tmp:
        s3_handler.download(input_file_key, tmp.name)

        con = duckdb.connect(f"md:{motherduck_db}?motherduck_token={motherduck_token}")

        con.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        con.sql(
            f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} AS "
            f"SELECT * FROM read_parquet('{tmp.name}') WHERE 1=0"
        )
        con.sql(
            f"DELETE FROM {schema_name}.{table_name} "
            f"WHERE code IN (SELECT code FROM read_parquet('{tmp.name}'))"
        )
        con.sql(f"INSERT INTO {schema_name}.{table_name} SELECT * FROM read_parquet('{tmp.name}')")

        con.close()

    logger.info(f"Upsert complete: {input_file_key} → {schema_name}.{table_name}")
