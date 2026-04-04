import os
import duckdb
import logging
import tempfile
from common.s3 import S3FileHandler

logger = logging.getLogger(__name__)


def handle(input_file_key, table_name, schema_name, key_column="code"):
    """Download a transformed delta parquet from S3 and upsert it into MotherDuck.

    Uses a DELETE + INSERT pattern keyed on `key_column` (default: `code`):
    - Existing rows with matching keys are replaced (updated).
    - New rows are inserted.

    Atomicité :
        Le DELETE et l'INSERT sont exécutés dans une transaction explicite.
        Si l'INSERT échoue (ex: incompatibilité de types), le ROLLBACK annule
        automatiquement le DELETE — la table reste dans son état d'origine.
        Cela évite la perte de données dans le cas où le DELETE aurait supprimé
        des enregistrements valides sans pouvoir les réinsérer.

    Args:
        input_file_key: S3 key of the transformed parquet to load.
        table_name: Target table name in MotherDuck.
        schema_name: Target schema name in MotherDuck.
        key_column: Column used to identify rows to delete before re-inserting (default: "code").
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

        # DELETE + INSERT dans une transaction pour garantir l'atomicité :
        # si l'INSERT échoue, le DELETE est annulé (ROLLBACK) et la table reste intacte.
        con.sql("BEGIN TRANSACTION")
        try:
            con.sql(
                f"DELETE FROM {schema_name}.{table_name} "
                f"WHERE {key_column} IN (SELECT {key_column} FROM read_parquet('{tmp.name}'))"
            )
            con.sql(f"INSERT INTO {schema_name}.{table_name} SELECT * FROM read_parquet('{tmp.name}')")
            con.sql("COMMIT")
        except Exception:
            con.sql("ROLLBACK")
            raise

        con.close()

    logger.info(f"Upsert complete: {input_file_key} → {schema_name}.{table_name}")
