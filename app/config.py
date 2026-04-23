import os
import duckdb
from dotenv import load_dotenv

load_dotenv()

APP_DIR = os.path.dirname(os.path.abspath(__file__))

DUCKDB_TOKEN = os.getenv("DUCKDB_TOKEN")
DUCKDB_DB = os.getenv("DUCKDB_DB")
TABLE_NAME   = os.getenv("TABLE_NAME",   "silver.products")
SILVER_SCHEMA = os.getenv("SILVER_SCHEMA", "silver")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
OPENAI_ALIAS_MODEL = os.getenv("OPENAI_ALIAS_MODEL", "gpt-4o-mini")
OPENAI_EMBEDDING_MODEL = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")
OPENAI_TIMEOUT_S = float(os.getenv("OPENAI_TIMEOUT_S", "20"))
ALIAS_CACHE_PATH = os.getenv(
    "ALIAS_CACHE_PATH",
    os.path.join(APP_DIR, "alias_normalization_cache.json"),
)

if not DUCKDB_TOKEN or not DUCKDB_DB:
    raise RuntimeError("DUCKDB_TOKEN et/ou DUCKDB_DB manquants dans .env")

CONN_STR = f"md:{DUCKDB_DB}?motherduck_token={DUCKDB_TOKEN}"

def get_conn() -> duckdb.DuckDBPyConnection:
    """Retourne une nouvelle connexion DuckDB à chaque appel (thread-safe)."""
    return duckdb.connect(CONN_STR, read_only=True)
