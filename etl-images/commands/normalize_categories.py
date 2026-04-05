import ast
import os
import hashlib
import logging
import requests
import pandas as pd
import numpy as np
from common.s3 import S3FileHandler

logger = logging.getLogger(__name__)

CATEGORIES_TXT_URL = (
    "https://raw.githubusercontent.com/openfoodfacts/"
    "openfoodfacts-server/main/taxonomies/food/categories.txt"
)


# ---------------------------------------------------------------------------
# Utilitaire — ID stable par hash
# ---------------------------------------------------------------------------

def _stable_id(name: str) -> int:
    """Génère un ID entier positif stable depuis un nom canonique OFF.

    16 hex chars = 64 bits → collision quasi-impossible même pour 80 000 entrées.
    Modulo 2^63 pour rester dans les limites du BIGINT signé de DuckDB.
    Même nom = même ID garanti entre initial load et tous les deltas futurs.
    """
    return int(hashlib.md5(name.encode()).hexdigest()[:16], 16) % (2 ** 63)


# ---------------------------------------------------------------------------
# 1. Téléchargement du fichier de référence
# ---------------------------------------------------------------------------

def _download_categories_txt(url):
    logger.info(f"Downloading categories taxonomy from {url}...")
    session = requests.Session()
    session.headers.update({"User-Agent": "FoodHealthAdvisor/1.0"})
    response = session.get(url, timeout=30)
    response.raise_for_status()
    return response.text


# ---------------------------------------------------------------------------
# 2. Parsing de la taxonomie
# ---------------------------------------------------------------------------

def _parse_taxonomy(text):
    """Parse categories.txt et retourne :
        canonical_map : {tag_quelconque -> tag_canonique}
        parent_map    : {tag_canonique -> tag_canonique_parent | None}
    """
    canonical_map = {}
    parent_map = {}

    current_canonical = None
    current_parents = []

    def _flush():
        nonlocal current_canonical, current_parents
        if current_canonical:
            parent_map[current_canonical] = (
                current_parents[0] if current_parents else None
            )
        current_canonical = None
        current_parents = []

    def _to_tag(lang, value):
        return f"{lang}:{value.strip().lower().replace(' ', '-')}"

    for raw_line in text.splitlines():
        line = raw_line.strip()

        if not line or line.startswith("#"):
            _flush()
            continue

        if line.startswith(("stopwords:", "synonyms:")):
            continue

        if line.startswith("< "):
            parent_tag = line[2:].strip().lower()
            current_parents.append(parent_tag)
            continue

        if ":" not in line:
            continue

        lang, rest = line.split(":", 1)
        lang = lang.strip()
        values = [v.strip() for v in rest.split(",") if v.strip()]

        if not values:
            continue

        if current_canonical is None and lang == "en":
            current_canonical = _to_tag(lang, values[0])
            canonical_map[current_canonical] = current_canonical

        if current_canonical:
            for val in values:
                synonym = _to_tag(lang, val)
                canonical_map.setdefault(synonym, current_canonical)

    _flush()

    logger.info(
        f"Taxonomy parsed: {len(parent_map)} canonical categories, "
        f"{len(canonical_map)} known tags (synonymes inclus)"
    )
    return canonical_map, parent_map


# ---------------------------------------------------------------------------
# 3. Normalisation d'une valeur categories_tags
# ---------------------------------------------------------------------------

def _to_list(tags):
    """Convertit categories_tags vers une liste Python, peu importe son type."""
    if tags is None:
        return []
    if isinstance(tags, float) and np.isnan(tags):
        return []
    if isinstance(tags, str):
        tags = tags.strip()
        if not tags or tags == "[]":
            return []
        try:
            parsed = ast.literal_eval(tags)
            return list(parsed) if isinstance(parsed, (list, tuple)) else []
        except (ValueError, SyntaxError):
            return []
    if isinstance(tags, list):
        return tags
    try:
        return [t for t in tags if isinstance(t, str)]
    except TypeError:
        return []


def _normalize_tags(tags, canonical_map):
    """Mappe chaque tag vers son canonique OFF, dédoublonne, logge les inconnus."""
    items = _to_list(tags)
    normalized = []
    seen = set()
    for tag in items:
        if not isinstance(tag, str):
            continue
        key = tag.strip().lower()
        canonical = canonical_map.get(key, key)
        if canonical not in seen:
            normalized.append(canonical)
            seen.add(canonical)
        if key not in canonical_map:
            logger.warning(f"Tag non reconnu dans la taxonomie OFF : '{tag}'")
    return normalized


# ---------------------------------------------------------------------------
# 4. Construction de la table categories (avec IDs stables)
# ---------------------------------------------------------------------------

def _build_categories_table(all_tags, parent_map):
    """Table categories avec IDs stables par hash.

    category_id        : hash MD5 (64 bits, mod 2^63) du category_name
    category_name      : tag canonique OFF (clé naturelle lisible)
    parent_category_id : hash MD5 du parent, ou None si racine

    Les IDs stables garantissent que les FK dans product_categories restent
    cohérentes entre l'initial load et tous les deltas futurs.
    """
    tags_to_include = set(all_tags)
    for tag in list(all_tags):
        parent = parent_map.get(tag)
        while parent and parent not in tags_to_include:
            tags_to_include.add(parent)
            parent = parent_map.get(parent)

    tag_to_id = {tag: _stable_id(tag) for tag in tags_to_include}

    rows = []
    for tag, cat_id in tag_to_id.items():
        parent_tag = parent_map.get(tag)
        rows.append({
            "category_id":        cat_id,
            "category_name":      tag,
            "parent_category_id": tag_to_id.get(parent_tag) if parent_tag in tag_to_id else None,
        })

    if not rows:
        return pd.DataFrame(
            columns=["category_id", "category_name", "parent_category_id"]
        ), {}

    return pd.DataFrame(rows), tag_to_id


# ---------------------------------------------------------------------------
# 5. Point d'entrée principal
# ---------------------------------------------------------------------------

def handle(
    input_file_key,
    categories_output_key,
    product_categories_output_key,
):
    s3_bucket     = os.environ["S3_BUCKET"]
    s3_endpoint   = os.environ["S3_ENDPOINT"]
    s3_access_key = os.environ["S3_ACCESS_KEY"]
    s3_secret_key = os.environ["S3_SECRET_KEY"]

    logger.info(f"Normalizing categories from {input_file_key}...")

    s3_handler = S3FileHandler(s3_bucket, s3_endpoint, s3_access_key, s3_secret_key)

    raw = s3_handler.download_to_memory(input_file_key)
    df  = pd.read_parquet(raw)

    if "categories_tags" not in df.columns:
        logger.error(
            "COLONNE 'categories_tags' ABSENTE DU PARQUET. "
            f"Colonnes disponibles : {df.columns.tolist()}"
        )
        s3_handler.upload_dataframe(
            pd.DataFrame(columns=["category_id", "category_name", "parent_category_id"]),
            categories_output_key,
        )
        s3_handler.upload_dataframe(
            pd.DataFrame(columns=["code", "category_id"]),
            product_categories_output_key,
        )
        return

    col = df["categories_tags"]
    sample = col.dropna().head(3)
    logger.info(
        f"[DIAG] categories_tags — dtype={col.dtype} | "
        f"non-null={col.notna().sum()}/{len(col)} | "
        f"sample types={[type(v).__name__ for v in sample]} | "
        f"sample values={[repr(v)[:60] for v in sample]}"
    )

    categories_txt = _download_categories_txt(CATEGORIES_TXT_URL)
    canonical_map, parent_map = _parse_taxonomy(categories_txt)

    df["categories_tags"] = df["categories_tags"].apply(
        lambda tags: _normalize_tags(tags, canonical_map)
    )

    all_tags = set()
    for tags in df["categories_tags"]:
        all_tags.update(tags)
    logger.info(f"Unique normalized categories: {len(all_tags)}")

    df_categories, tag_to_id = _build_categories_table(all_tags, parent_map)

    junction_rows = [
        {"code": row["code"], "category_id": tag_to_id[tag]}
        for _, row in df[["code", "categories_tags"]].iterrows()
        for tag in row["categories_tags"]
        if tag in tag_to_id
    ]
    df_product_categories = pd.DataFrame(
        junction_rows if junction_rows else [],
        columns=["code", "category_id"],
    )

    s3_handler.upload_dataframe(df_categories, categories_output_key)
    logger.info(f"categories uploaded → {categories_output_key} ({len(df_categories)} records)")

    s3_handler.upload_dataframe(df_product_categories, product_categories_output_key)
    logger.info(f"product_categories uploaded → {product_categories_output_key} ({len(df_product_categories)} records)")
