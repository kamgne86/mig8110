import ast
import os
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
    """Convertit categories_tags vers une liste Python, peu importe son type.

    Le parquet peut stocker cette colonne sous plusieurs formes :
      - None / float NaN       → vide
      - str  "['en:x', ...]"   → ast.literal_eval
      - list ['en:x', ...]     → déjà bon
      - np.ndarray             → list()
      - tout autre iterable    → list()
    """
    if tags is None:
        return []
    # NaN float (pandas remplace les nulls par NaN pour dtype=object)
    if isinstance(tags, float) and np.isnan(tags):
        return []
    # String repr de liste (cas typique après lecture CSV ou parquet object)
    if isinstance(tags, str):
        tags = tags.strip()
        if not tags or tags == "[]":
            return []
        try:
            parsed = ast.literal_eval(tags)
            return list(parsed) if isinstance(parsed, (list, tuple)) else []
        except (ValueError, SyntaxError):
            return []
    # Déjà une liste Python
    if isinstance(tags, list):
        return tags
    # numpy array ou autre séquence
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
# 4. Construction de la table categories (avec hiérarchie)
# ---------------------------------------------------------------------------

def _build_categories_table(all_tags, parent_map):
    """Table categories avec category_id et parent_category_id."""
    tags_to_include = set(all_tags)
    for tag in list(all_tags):
        parent = parent_map.get(tag)
        while parent and parent not in tags_to_include:
            tags_to_include.add(parent)
            parent = parent_map.get(parent)

    sorted_tags = sorted(tags_to_include)
    tag_to_id = {
        tag: idx + 1 for idx, tag in enumerate(sorted_tags)
    }

    rows = []
    for tag, cat_id in tag_to_id.items():
        parent_tag = parent_map.get(tag)
        rows.append({
            "category_id":        cat_id,
            "category_name":      tag,
            "parent_category_id": tag_to_id.get(parent_tag) if parent_tag else None,
        })

    return pd.DataFrame(rows), tag_to_id


# ---------------------------------------------------------------------------
# 5. Point d'entrée principal
# ---------------------------------------------------------------------------

def handle(
    input_file_key,
    products_output_key,
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

    # -----------------------------------------------------------------------
    # DIAGNOSTIC — affiche l'état réel de categories_tags dans le parquet
    # -----------------------------------------------------------------------
    if "categories_tags" not in df.columns:
        logger.error(
            "COLONNE 'categories_tags' ABSENTE DU PARQUET. "
            f"Colonnes disponibles : {df.columns.tolist()}"
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
    # -----------------------------------------------------------------------

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

    df_products = df.drop(columns=["categories_tags"])

    s3_handler.upload_dataframe(df_products, products_output_key)
    logger.info(f"products uploaded → {products_output_key} ({len(df_products)} records)")

    s3_handler.upload_dataframe(df_categories, categories_output_key)
    logger.info(f"categories uploaded → {categories_output_key} ({len(df_categories)} records)")

    s3_handler.upload_dataframe(df_product_categories, product_categories_output_key)
    logger.info(f"product_categories uploaded → {product_categories_output_key} ({len(df_product_categories)} records)")