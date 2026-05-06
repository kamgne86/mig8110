import os
import duckdb
from dotenv import dotenv_values, load_dotenv

APP_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(APP_DIR, ".env")

load_dotenv(ENV_PATH, override=False)
ENV_FILE_VALUES = dotenv_values(ENV_PATH)


def get_setting(name: str, default: str | None = None) -> str | None:
    value = ENV_FILE_VALUES.get(name)
    if value not in (None, ""):
        return value
    env_value = os.getenv(name)
    if env_value not in (None, ""):
        return env_value
    return default

DUCKDB_TOKEN = get_setting("DUCKDB_TOKEN")
DUCKDB_DB = get_setting("DUCKDB_DB")
TABLE_NAME = get_setting("TABLE_NAME", "silver.products")
SILVER_SCHEMA = get_setting("SILVER_SCHEMA", "silver")
OPENAI_API_KEY = get_setting("OPENAI_API_KEY")
OPENAI_BASE_URL = str(get_setting("OPENAI_BASE_URL", "https://api.openai.com/v1")).rstrip("/")
OPENAI_EMBEDDING_MODEL = str(get_setting("OPENAI_EMBEDDING_MODEL", "text-embedding-3-large"))
OPENAI_NORMALIZATION_MODEL = str(get_setting("OPENAI_NORMALIZATION_MODEL", "gpt-4.1"))
OPENAI_LLM_MODEL = str(get_setting("OPENAI_LLM_MODEL", "gpt-5"))
OPENAI_TIMEOUT_S = float(str(get_setting("OPENAI_TIMEOUT_S", "20")))

if not DUCKDB_TOKEN or not DUCKDB_DB:
    raise RuntimeError("DUCKDB_TOKEN et/ou DUCKDB_DB manquants dans .env")

CONN_STR = f"md:{DUCKDB_DB}?motherduck_token={DUCKDB_TOKEN}"

def get_conn() -> duckdb.DuckDBPyConnection:
    """Retourne une nouvelle connexion DuckDB à chaque appel (thread-safe)."""
    return duckdb.connect(CONN_STR, read_only=True)
