import os
import duckdb
import logging
import tempfile
from common.s3 import S3FileHandler

logging.basicConfig(level=logging.INFO)


def handle(input_file_key, table_name, schema_name):
    """UPSERT parquet records into a Silver table using DELETE + INSERT by code.

    - Records whose code already exists in the table are replaced (UPDATE).
    - Records whose code is new are added (INSERT).
    """
    logging.info(f"Merging {input_file_key} into {schema_name}.{table_name}...")

    s3_bucket = os.environ["S3_BUCKET"]
    s3_endpoint = os.environ["S3_ENDPOINT"]
    s3_access_key = os.environ["S3_ACCESS_KEY"]
    s3_secret_key = os.environ["S3_SECRET_KEY"]

    motherduck_token = os.environ["DUCKDB_TOKEN"]
    motherduck_db = os.environ["DUCKDB_DB"]

    s3_handler = S3FileHandler(s3_bucket, s3_endpoint, s3_access_key, s3_secret_key)
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=True) as tmp:
        s3_handler.download(input_file_key, tmp.name)

        con = duckdb.connect(f"md:{motherduck_db}?motherduck_token={motherduck_token}")
        # Delete existing rows whose codes appear in the incoming batch, then insert all
        con.sql(f"""
            DELETE FROM {schema_name}.{table_name}
            WHERE code IN (SELECT code FROM read_parquet('{tmp.name}'))
        """)
        con.sql(f"""
            INSERT INTO {schema_name}.{table_name}
            SELECT * FROM read_parquet('{tmp.name}')
        """)
        con.close()

    logging.info(f"Merge complete for '{schema_name}.{table_name}'")
