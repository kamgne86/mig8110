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

    is_jsonl = input_file_key.endswith(".jsonl")
    suffix = ".jsonl" if is_jsonl else ".parquet"

    s3_handler = S3FileHandler(s3_bucket, s3_endpoint, s3_access_key, s3_secret_key)
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=True) as tmp:
        s3_handler.download(input_file_key, tmp.name)

        read_fn = f"read_json_auto('{tmp.name}')" if is_jsonl else f"read_parquet('{tmp.name}')"

        con = duckdb.connect(f"md:{motherduck_db}?motherduck_token={motherduck_token}")
        con.sql(f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} AS SELECT * FROM {read_fn} WHERE 1=0")
        con.sql(f"INSERT INTO {schema_name}.{table_name} SELECT * FROM {read_fn}")
        con.close()

    logger.info(f"Data inserted into DuckDB table '{schema_name}.{table_name}'")
