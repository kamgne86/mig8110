import os
import gzip
import json
import logging
import tempfile
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from urllib.parse import urljoin
from common.s3 import S3FileHandler

logger = logging.getLogger(__name__)

CHUNK_SIZE = 50 * 1024 * 1024  # 50 MB
BATCH_SIZE = 500


def _matches_country(tags, country):
    """Check if any of the countries_tags contains the given country string."""
    return isinstance(tags, list) and any(country.lower() in tag.lower() for tag in tags)


def _serialize_record(record):
    """Convertit toutes les valeurs d'un record en types simples (str, int, float, None).

    - lists / dicts  → JSON string
    - None / NaN     → None
    - tout le reste  → str
    Garantit qu'aucune colonne ne peut avoir des types mixtes entre records.
    """
    out = {}
    for key, val in record.items():
        if val is None:
            out[key] = None
        elif isinstance(val, (list, dict)):
            out[key] = json.dumps(val, ensure_ascii=False)
        elif isinstance(val, float):
            out[key] = None if val != val else val   # NaN → None
        elif isinstance(val, (int, bool)):
            out[key] = val
        else:
            out[key] = str(val)
    return out


def _build_schema(all_columns):
    """Construit un schéma PyArrow fixe : toutes les colonnes en large_string.

    Utiliser exclusivement large_string évite tout conflit de type entre batches
    (int64 vs double vs large_string pour la même colonne selon le record).
    Le casting vers les vrais types numériques se fait en aval dans transform_delta.
    """
    return pa.schema([pa.field(col, pa.large_utf8()) for col in sorted(all_columns)])


def _batch_to_table(batch, schema):
    """Convertit un batch de records en pa.Table aligné sur le schéma fixe.

    - Colonnes manquantes dans ce batch → remplies de None
    - Colonnes en trop (absentes du schéma) → ignorées
    - Toutes les valeurs castées en str pour correspondre à large_string
    """
    # Aligner les colonnes sur le schéma
    aligned = {col: [] for col in schema.names}
    for record in batch:
        for col in schema.names:
            val = record.get(col)
            if val is None:
                aligned[col].append(None)
            else:
                aligned[col].append(str(val))

    arrays = [pa.array(aligned[col], type=pa.large_utf8()) for col in schema.names]
    return pa.table(dict(zip(schema.names, arrays)), schema=schema)


def _download_and_filter(session, url, country):
    """Télécharge, filtre par pays et écrit en parquet par batches.

    Stratégie mémoire :
      1. Téléchargement en chunks de 50 MB → fichier tmp .gz
      2. Décompression ligne par ligne → jamais tout en RAM
      3. Écriture parquet par batch de BATCH_SIZE records
      4. Schéma fixé sur le premier batch, tous les suivants alignés dessus

    Returns:
        (out_path, total_records)
    """
    head = session.head(url, timeout=30)
    head.raise_for_status()
    total_size = int(head.headers["Content-Length"])
    logger.info(
        f"Downloading {url} ({total_size / 1024 / 1024:.0f} MB) "
        f"in {CHUNK_SIZE // 1024 // 1024} MB chunks..."
    )

    with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as tmp:
        tmp_path = tmp.name
        downloaded = 0
        while downloaded < total_size:
            end = min(downloaded + CHUNK_SIZE - 1, total_size - 1)
            resp = session.get(url, headers={"Range": f"bytes={downloaded}-{end}"}, timeout=120)
            resp.raise_for_status()
            tmp.write(resp.content)
            downloaded += len(resp.content)
            logger.info(
                f"  {downloaded / total_size * 100:.1f}% "
                f"({downloaded // 1024 // 1024} MB / {total_size // 1024 // 1024} MB)"
            )

    out_tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    out_path = out_tmp.name
    out_tmp.close()

    try:
        # --- Passe 1 : collecter toutes les colonnes présentes dans les records canada ---
        # Nécessaire pour construire un schéma fixe avant d'ouvrir le ParquetWriter.
        logger.info("Pass 1 — scanning column names...")
        all_columns = set()
        canada_lines = []

        with gzip.open(tmp_path, "rt", encoding="utf-8") as gz:
            for line in gz:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                if not _matches_country(record.get("countries_tags"), country):
                    continue
                serialized = _serialize_record(record)
                all_columns.update(serialized.keys())
                canada_lines.append(serialized)

        total_records = len(canada_lines)
        logger.info(f"Found {total_records} {country} records, {len(all_columns)} distinct columns.")

        if total_records == 0:
            # Écrire un parquet vide avec schéma minimal
            empty = pa.table({"code": pa.array([], type=pa.large_utf8())})
            pq.write_table(empty, out_path)
            return out_path, 0

        schema = _build_schema(all_columns)

        # --- Passe 2 : écriture en batches avec schéma fixe ---
        logger.info("Pass 2 — writing parquet batches...")
        with pq.ParquetWriter(out_path, schema) as writer:
            for i in range(0, total_records, BATCH_SIZE):
                batch = canada_lines[i: i + BATCH_SIZE]
                table = _batch_to_table(batch, schema)
                writer.write_table(table)
                logger.info(
                    f"  Batch {i // BATCH_SIZE + 1} written "
                    f"({min(i + BATCH_SIZE, total_records)}/{total_records})"
                )

        return out_path, total_records

    finally:
        os.unlink(tmp_path)


def handle(filename, output_file_key, base_url, country="canada"):
    """Télécharge un fichier delta, filtre par pays et uploade en parquet sur S3."""
    s3_bucket    = os.environ["S3_BUCKET"]
    s3_endpoint  = os.environ["S3_ENDPOINT"]
    s3_access_key = os.environ["S3_ACCESS_KEY"]
    s3_secret_key = os.environ["S3_SECRET_KEY"]

    session = requests.Session()
    session.headers.update({"User-Agent": "FoodHealthAdvisor/1.0"})

    url = urljoin(base_url, filename)
    logger.info(f"Processing delta file: {filename}")

    parquet_path, total_records = _download_and_filter(session, url, country)

    if total_records == 0:
        logger.warning(f"No {country} records found in {filename}, uploading empty parquet.")

    s3_handler = S3FileHandler(s3_bucket, s3_endpoint, s3_access_key, s3_secret_key)
    try:
        with open(parquet_path, "rb") as f:
            s3_handler.upload_from_memory(f, output_file_key)
    finally:
        os.unlink(parquet_path)

    logger.info(f"Uploaded {output_file_key} ({total_records} records).")
    