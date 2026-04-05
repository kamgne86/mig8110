import ast
import os
import hashlib
import logging
import requests
import pandas as pd
import numpy as np
from common.s3 import S3FileHandler

logger = logging.getLogger(__name__)

INGREDIENTS_TXT_URL = (
    "https://raw.githubusercontent.com/openfoodfacts/"
    "openfoodfacts-server/main/taxonomies/ingredients.txt"
)


# ---------------------------------------------------------------------------
# Utilitaire — ID stable par hash
# ---------------------------------------------------------------------------

def _stable_id(name: str) -> int:
    """Génère un ID entier positif stable depuis un nom canonique OFF.

    16 hex chars = 64 bits → collision quasi-impossible même pour 80 000 ingrédients.
    Modulo 2^63 pour rester dans les limites du BIGINT signé de DuckDB.
    Même nom = même ID garanti entre initial load et tous les deltas futurs.
    """
    return int(hashlib.md5(name.encode()).hexdigest()[:16], 16) % (2 ** 63)


# ---------------------------------------------------------------------------
# 1. Téléchargement du fichier de référence
# ---------------------------------------------------------------------------

def _download_ingredients_txt(url: str) -> str:
    logger.info(f"Downloading ingredients taxonomy from {url}...")
    session = requests.Session()
    session.headers.update({"User-Agent": "FoodHealthAdvisor/1.0"})
    response = session.get(url, timeout=60)
    response.raise_for_status()
    return response.text


# ---------------------------------------------------------------------------
# 2. Parsing de la taxonomie
# ---------------------------------------------------------------------------

def _is_language_code(lang: str) -> bool:
    """Retourne True si lang est un code de langue ISO (2-3 lettres).

    Permet de distinguer les lignes de traduction (en, fr, de)
    des lignes de propriétés (vegan, nova, allergens, description, wikidata...)
    sans avoir à les lister explicitement — robuste aux nouvelles propriétés OFF.
    """
    return lang.isalpha() and 2 <= len(lang) <= 3


def _parse_taxonomy(text: str) -> dict:
    """Parse ingredients.txt et retourne canonical_map.

    canonical_map : {tag_quelconque -> tag_canonique}
        Exemple : 'fr:huile-de-soja' -> 'en:soybean-oil'
    """
    canonical_map: dict[str, str] = {}
    current_canonical: str | None = None

    def _flush():
        nonlocal current_canonical
        current_canonical = None

    def _to_tag(lang: str, value: str) -> str:
        return f"{lang}:{value.strip().lower().replace(' ', '-')}"

    for raw_line in text.splitlines():
        line = raw_line.strip()

        if not line or line.startswith("#"):
            _flush()
            continue

        if line.startswith(("stopwords:", "synonyms:", "< ")):
            continue

        if ":" not in line:
            continue

        lang, rest = line.split(":", 1)
        lang = lang.strip()

        if not _is_language_code(lang):
            continue

        values = [v.strip() for v in rest.split(",") if v.strip()]

        if not values:
            continue

        if current_canonical is None and lang == "en":
            current_canonical = _to_tag(lang, values[0])
            canonical_map[current_canonical] = current_canonical

        if current_canonical:
            for val in values:
                canonical_map.setdefault(_to_tag(lang, val), current_canonical)

    _flush()

    logger.info(
        f"Taxonomy parsed: {sum(1 for k,v in canonical_map.items() if k==v)} canonical ingredients, "
        f"{len(canonical_map)} known tags (synonymes inclus)"
    )
    return canonical_map


# ---------------------------------------------------------------------------
# 3. Conversion de la colonne ingredients_tags
# ---------------------------------------------------------------------------

def _to_list(tags) -> list[str]:
    """Convertit ingredients_tags en liste Python peu importe son type."""
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


def _normalize_tags(tags, canonical_map: dict) -> list[str]:
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
# 4. Construction de la table ingredients (avec IDs stables)
# ---------------------------------------------------------------------------

def _build_ingredients_table(all_tags: set[str]) -> tuple[pd.DataFrame, dict]:
    """Table ingredients avec IDs stables par hash.

    ingredient_id   : hash MD5 (64 bits, mod 2^63) du ingredient_name
    ingredient_name : tag canonique OFF (clé naturelle lisible)

    Les IDs stables garantissent que les FK dans product_ingredients restent
    cohérentes entre l'initial load et tous les deltas futurs.
    """
    if not all_tags:
        return pd.DataFrame(columns=["ingredient_id", "ingredient_name"]), {}

    tag_to_id = {tag: _stable_id(tag) for tag in all_tags}

    rows = [
        {"ingredient_id": iid, "ingredient_name": tag}
        for tag, iid in tag_to_id.items()
    ]

    return pd.DataFrame(sorted(rows, key=lambda r: r["ingredient_name"])), tag_to_id


# ---------------------------------------------------------------------------
# 5. Point d'entrée principal
# ---------------------------------------------------------------------------

def handle(
    input_file_key: str,
    ingredients_output_key: str,
    product_ingredients_output_key: str,
) -> None:
    """Normalise ingredients_tags et produit deux parquets distincts sur S3.

    Remplace la colonne ingredients_tags monolithique par :
        ingredients          : référentiel OFF (ingredient_id, ingredient_name)
        product_ingredients  : table de jonction Many-to-Many (code, ingredient_id)

    La table products est produite par finalize_products après l'exécution
    parallèle de normalize_categories et normalize_ingredients.
    """
    s3_bucket     = os.environ["S3_BUCKET"]
    s3_endpoint   = os.environ["S3_ENDPOINT"]
    s3_access_key = os.environ["S3_ACCESS_KEY"]
    s3_secret_key = os.environ["S3_SECRET_KEY"]

    logger.info(f"Normalizing ingredients from {input_file_key}...")

    s3_handler = S3FileHandler(s3_bucket, s3_endpoint, s3_access_key, s3_secret_key)

    raw = s3_handler.download_to_memory(input_file_key)
    df  = pd.read_parquet(raw)

    if "ingredients_tags" not in df.columns:
        logger.error(
            "COLONNE 'ingredients_tags' ABSENTE DU PARQUET. "
            f"Colonnes disponibles : {df.columns.tolist()}"
        )
        s3_handler.upload_dataframe(
            pd.DataFrame(columns=["ingredient_id", "ingredient_name"]),
            ingredients_output_key,
        )
        s3_handler.upload_dataframe(
            pd.DataFrame(columns=["code", "ingredient_id"]),
            product_ingredients_output_key,
        )
        return

    col = df["ingredients_tags"]
    sample = col.dropna().head(3)
    logger.info(
        f"[DIAG] ingredients_tags — dtype={col.dtype} | "
        f"non-null={col.notna().sum()}/{len(col)} | "
        f"sample types={[type(v).__name__ for v in sample]} | "
        f"sample values={[repr(v)[:60] for v in sample]}"
    )

    ingredients_txt = _download_ingredients_txt(INGREDIENTS_TXT_URL)
    canonical_map   = _parse_taxonomy(ingredients_txt)

    df["ingredients_tags"] = df["ingredients_tags"].apply(
        lambda tags: _normalize_tags(tags, canonical_map)
    )

    all_tags: set[str] = set()
    for tags in df["ingredients_tags"]:
        all_tags.update(tags)
    logger.info(f"Unique normalized ingredients: {len(all_tags)}")

    df_ingredients, tag_to_id = _build_ingredients_table(all_tags)

    junction_rows = [
        {"code": row["code"], "ingredient_id": tag_to_id[tag]}
        for _, row in df[["code", "ingredients_tags"]].iterrows()
        for tag in row["ingredients_tags"]
        if tag in tag_to_id
    ]
    df_product_ingredients = pd.DataFrame(
        junction_rows if junction_rows else [],
        columns=["code", "ingredient_id"],
    )

    s3_handler.upload_dataframe(df_ingredients, ingredients_output_key)
    logger.info(f"ingredients uploaded → {ingredients_output_key} ({len(df_ingredients)} records)")

    s3_handler.upload_dataframe(df_product_ingredients, product_ingredients_output_key)
    logger.info(f"product_ingredients uploaded → {product_ingredients_output_key} ({len(df_product_ingredients)} records)")
