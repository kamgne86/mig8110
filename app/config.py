import os
import duckdb
from dotenv import load_dotenv

load_dotenv()

DUCKDB_TOKEN = os.getenv("DUCKDB_TOKEN")
DUCKDB_DB = os.getenv("DUCKDB_DB")
TABLE_NAME = "raw.products"

if not DUCKDB_TOKEN or not DUCKDB_DB:
    raise RuntimeError("DUCKDB_TOKEN et/ou DUCKDB_DB manquants dans .env")

conn_str = f"md:{DUCKDB_DB}?motherduck_token={DUCKDB_TOKEN}"
db = duckdb.connect(conn_str, read_only=True)
