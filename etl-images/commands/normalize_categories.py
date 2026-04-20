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

    category_id    : hash MD5 (64 bits, mod 2^63) du category_name
    category_name  : tag canonique OFF (clé naturelle lisible)

    Les IDs stables garantissent que les FK dans products.categorie_principale
    et ancetre_categories restent cohérentes entre l'initial load et tous les
    deltas futurs.

    La relation parent-enfant n'est plus stockée ici : elle est dérivable
    depuis ancetre_categories (distance = 1 pour le parent direct).
    """
    tags_to_include = set(all_tags)
    for tag in list(all_tags):
        parent = parent_map.get(tag)
        while parent and parent not in tags_to_include:
            tags_to_include.add(parent)
            parent = parent_map.get(parent)

    tag_to_id = {tag: _stable_id(tag) for tag in tags_to_include}

    if not tag_to_id:
        return pd.DataFrame({
            "category_id":   pd.array([], dtype=pd.Int64Dtype()),
            "category_name": pd.array([], dtype="string"),
        }), {}

    tags = list(tag_to_id.keys())
    df = pd.DataFrame({
        "category_id":   pd.array(list(tag_to_id.values()), dtype=pd.Int64Dtype()),
        "category_name": tags,
    })
    return df, tag_to_id


# ---------------------------------------------------------------------------
# 5. Construction de la table ancetre_categories (closure table)
# ---------------------------------------------------------------------------

def _build_ancetre_categories(tag_to_id, parent_map):
    """Table de fermeture des ancêtres avec distance.

    category_id        : hash MD5 d'une catégorie descendante
    category_id_parent : hash MD5 d'un de ses ancêtres (parent, grand-parent, ...)
    distance           : 1=parent direct, 2=grand-parent, 3=arrière-grand-parent...

    Remplace la table product_categories (Many-to-Many) et permet de retrouver
    tous les descendants d'une catégorie via une seule requête SQL.

    Exemple pour en:maple-syrups :
        (en:maple-syrups, en:syrups,     1)
        (en:maple-syrups, en:sweeteners, 2)
        (en:maple-syrups, en:food,       3)

    La self-reference (distance=0) n'est pas incluse : seuls les ancêtres
    stricts (distance ≥ 1) sont stockés, conformément au MCD validé.
    """
    rows = []
    for tag, category_id in tag_to_id.items():
        parent = parent_map.get(tag)
        distance = 1
        while parent:
            if parent in tag_to_id:
                rows.append({
                    "category_id":        category_id,
                    "category_id_parent": tag_to_id[parent],
                    "distance":           distance,
                })
            parent = parent_map.get(parent)
            distance += 1

    if not rows:
        return pd.DataFrame({
            "category_id":        pd.array([], dtype=pd.Int64Dtype()),
            "category_id_parent": pd.array([], dtype=pd.Int64Dtype()),
            "distance":           pd.array([], dtype=pd.Int32Dtype()),
        })

    df = pd.DataFrame(rows)
    df["category_id"]        = df["category_id"].astype(pd.Int64Dtype())
    df["category_id_parent"] = df["category_id_parent"].astype(pd.Int64Dtype())
    df["distance"]           = df["distance"].astype(pd.Int32Dtype())
    return df


# ---------------------------------------------------------------------------
# 6. Construction de la jonction code -> categorie_principale
# ---------------------------------------------------------------------------

def _build_categorie_principale_table(df, tag_to_id):
    """Table de jonction (code, categorie_principale).

    code                 : clé du produit (FK vers products.code)
    categorie_principale : hash MD5 du dernier tag de categories_tags
                           (= catégorie la plus spécifique du produit), ou None
                           si le produit n'a aucune catégorie.

    Parquet intermédiaire consommé par finalize_products pour merger
    categorie_principale dans la table products finale. Ce découpage évite
    toute écriture concurrente sur le parquet transformé pendant que
    normalize_ingredients tourne en parallèle.
    """
    categorie_principale = df["categories_tags"].apply(
        lambda tags: tag_to_id[tags[-1]] if tags and tags[-1] in tag_to_id else None
    )

    out = pd.DataFrame({
        "code":                 df["code"],
        "categorie_principale": pd.array(categorie_principale, dtype=pd.Int64Dtype()),
    })
    return out


# ---------------------------------------------------------------------------
# 7. Point d'entrée principal
# ---------------------------------------------------------------------------

def handle(
    input_file_key,
    categories_output_key,
    ancetre_categories_output_key,
    categorie_principale_output_key,
):
    """Normalise categories_tags et produit trois parquets sur S3.

    Remplace la colonne categories_tags monolithique par :
        categories            : référentiel OFF (category_id, category_name)
        ancetre_categories    : table de fermeture des ancêtres avec distance
        categorie_principale  : jonction (code, categorie_principale) mergée
                                ensuite par finalize_products dans products

    La table products finale est produite par finalize_products après
    l'exécution parallèle de normalize_categories et normalize_ingredients.
    """
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
            pd.DataFrame(columns=["category_id", "category_name"]),
            categories_output_key,
        )
        s3_handler.upload_dataframe(
            pd.DataFrame(columns=["category_id", "category_id_parent", "distance"]),
            ancetre_categories_output_key,
        )
        s3_handler.upload_dataframe(
            pd.DataFrame(columns=["code", "categorie_principale"]),
            categorie_principale_output_key,
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

    df_ancetre_categories = _build_ancetre_categories(tag_to_id, parent_map)

    df_categorie_principale = _build_categorie_principale_table(df, tag_to_id)

    s3_handler.upload_dataframe(df_categories, categories_output_key)
    logger.info(f"categories uploaded → {categories_output_key} ({len(df_categories)} records)")

    s3_handler.upload_dataframe(df_ancetre_categories, ancetre_categories_output_key)
    logger.info(
        f"ancetre_categories uploaded → {ancetre_categories_output_key} "
        f"({len(df_ancetre_categories)} records)"
    )

    s3_handler.upload_dataframe(df_categorie_principale, categorie_principale_output_key)
    logger.info(
        f"categorie_principale uploaded → {categorie_principale_output_key} "
        f"({len(df_categorie_principale)} records)"
    )
