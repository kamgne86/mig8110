import os
import duckdb
from dotenv import load_dotenv

load_dotenv()

DUCKDB_TOKEN = os.getenv("DUCKDB_TOKEN")
DUCKDB_DB = os.getenv("DUCKDB_DB")
TABLE_NAME   = os.getenv("TABLE_NAME",   "silver.products")
SILVER_SCHEMA = os.getenv("SILVER_SCHEMA", "silver")

if not DUCKDB_TOKEN or not DUCKDB_DB:
    raise RuntimeError("DUCKDB_TOKEN et/ou DUCKDB_DB manquants dans .env")

CONN_STR = f"md:{DUCKDB_DB}?motherduck_token={DUCKDB_TOKEN}"

def get_conn() -> duckdb.DuckDBPyConnection:
    """Retourne une nouvelle connexion DuckDB à chaque appel (thread-safe)."""
    return duckdb.connect(CONN_STR, read_only=True)
