
"""
normalize_ingredients.py
========================
Ce module normalise la colonne 'ingredients' du fichier parquet transformé
et produit 4 tables de sortie :

    1. ingredients           — Référentiel unique de tous les ingrédients
                               (ingredient_id [BIGINT], ingredient_name)

    2. product_ingredients   — Jonction produit ↔ ingrédient (niveau 1 seulement)
                               (code, ingredient_id [BIGINT FK], ingredient_order, role)

    3. sous_ingredients      — Composition des ingrédients composés (niveau 2+)
                               (ingredient_id [BIGINT FK], sous_ingredient_id [BIGINT FK],
                                sous_ingredient_name, rang)

    4. ingredient_alias      — Variantes textuelles d'un même ingrédient
                               (ingredient_id [BIGINT FK], alias_name)

Logique des identifiants :
    En interne, le pipeline utilise des tags OFF (ex: "en:sugar") pour
    la résolution canonique. En sortie, chaque tag unique est remplacé
    par un ID stable (hash MD5 64 bits, mod 2^63), identique entre
    initial load et tous les deltas futurs.

Logique de nommage :
    ingredient_name = slug extrait du tag, tirets remplacés par espaces
    Exemple : en:coconut-cream → coconut cream
"""

import os
import json
import hashlib
import logging
import re
import unicodedata
from typing import Any

import numpy as np
import pandas as pd
import requests

from common.s3 import S3FileHandler

logger = logging.getLogger(__name__)


# ===========================================================================
# CONSTANTES — URLs des taxonomies Open Food Facts
# ===========================================================================

INGREDIENTS_TXT_URL = (
    "https://raw.githubusercontent.com/openfoodfacts/"
    "openfoodfacts-server/main/taxonomies/food/ingredients.txt"
)

ADDITIVES_TXT_URL = (
    "https://raw.githubusercontent.com/openfoodfacts/"
    "openfoodfacts-server/main/taxonomies/additives.txt"
)

ADDITIVE_CLASSES_TXT_URL = (
    "https://raw.githubusercontent.com/openfoodfacts/"
    "openfoodfacts-server/main/taxonomies/additives_classes.txt"
)


# ===========================================================================
# HELPERS GÉNÉRAUX — Fonctions utilitaires de nettoyage de texte
# ===========================================================================

def _is_null(value: Any) -> bool:
    """Vérifie si une valeur est nulle (None ou NaN)."""
    return value is None or (isinstance(value, float) and np.isnan(value))


def _strip_accents(value: str) -> str:
    """
    Retire les accents d'une chaîne Unicode.
    Exemple : 'café' → 'cafe', 'blé' → 'ble'
    """
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _clean_text(value: str) -> str:
    """
    Nettoie un texte brut :
    - Retire les accents
    - Remplace les underscores par des espaces
    - Supprime les balises HTML, parenthèses, guillemets
    - Ne garde que les caractères alphanumériques et quelques symboles
    - Normalise les espaces multiples

    Exemple : '_Soy_ Lecithin' → 'Soy Lecithin'
    """
    if not isinstance(value, str):
        return ""
    value = _strip_accents(value)
    value = value.replace("_", " ")
    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"[\(\)\[\]\{\}\"'`]", " ", value)
    value = re.sub(r"[^a-zA-Z0-9%+\-/,:.\s]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _slugify(value: str) -> str:
    """
    Transforme un texte en slug normalisé.
    Exemple : 'Whole Wheat Flour' → 'whole-wheat-flour'
    """
    value = _clean_text(value).lower()
    value = value.replace("/", " ")
    value = value.replace(",", " ")
    value = re.sub(r"\s+", "-", value)
    value = re.sub(r"-+", "-", value).strip("-")
    return value


def _canonical_name_from_id(tag_id: str) -> str:
    """
    Extrait la partie après le préfixe de langue.
    Exemple : 'en:coconut-cream' → 'coconut-cream'
    """
    return tag_id.split(":", 1)[1] if ":" in tag_id else tag_id


def _id_to_name(tag_id: str) -> str:
    """
    Convertit un tag OFF en nom lisible pour ingredient_name.
    C'est la SEULE fonction qui génère les noms d'ingrédients.

    Exemples :
        en:coconut-cream     → 'coconut cream'
        en:e150a             → 'e150a'
        fr:beurre-de-cacao   → 'beurre de cacao'
        en:acidity-regulator → 'acidity regulator'
    """
    raw = _canonical_name_from_id(tag_id)
    return raw.replace("-", " ").replace("_", " ")


def _is_language_code(lang: str) -> bool:
    """Vérifie si une chaîne est un code de langue ISO (2 ou 3 lettres)."""
    return lang.isalpha() and 2 <= len(lang) <= 3


def _safe_text(value: Any) -> str | None:
    """
    Nettoie un texte et retourne None si le résultat est vide.
    Exemple : '_Soy_ Lecithin' → 'Soy Lecithin'
    """
    if isinstance(value, str):
        cleaned = _clean_text(value)
        return cleaned if cleaned else None
    return None


def _normalize_alias_text(value: str | None) -> str | None:
    """
    Normalise un texte brut pour en faire un alias comparable.
    Exemple : 'Caramel Color' → 'caramel color'
    """
    if not value:
        return None
    cleaned = _clean_text(value).lower()
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or None


def _parse_ingredients_value(value: Any) -> list:
    """
    Convertit la valeur de la colonne 'ingredients' en liste Python.
    Gère : liste Python, chaîne JSON, None/NaN → [].
    """
    if _is_null(value):
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return []
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            logger.warning(f"Unable to parse ingredients JSON: {value[:120]}")
            return []
    return []


# ===========================================================================
# ID STABLE PAR HASH
# ===========================================================================

def _stable_id(tag: str) -> int:
    """Génère un ID entier positif stable depuis un tag OFF canonique.

    16 hex chars = 64 bits → collision quasi-impossible même pour 80 000 ingrédients.
    Modulo 2^63 pour rester dans les limites du BIGINT signé de DuckDB.
    Même tag = même ID garanti entre initial load et tous les deltas futurs.
    """
    return int(hashlib.md5(tag.encode()).hexdigest()[:16], 16) % (2 ** 63)


# ===========================================================================
# TÉLÉCHARGEMENT DES TAXONOMIES OFF
# ===========================================================================

def _download_taxonomy(url: str) -> str:
    """Télécharge un fichier taxonomie OFF depuis GitHub."""
    logger.info(f"Downloading taxonomy from {url}...")
    session = requests.Session()
    session.headers.update({"User-Agent": "FoodHealthAdvisor/1.0"})
    response = session.get(url, timeout=60)
    response.raise_for_status()
    return response.text


# ===========================================================================
# PARSING DES TAXONOMIES OFF
# ===========================================================================

def _parse_taxonomy_with_properties(
    text: str,
) -> tuple[dict[str, str], dict[str, dict[str, str]]]:
    """
    Parse un fichier taxonomie OFF et retourne :

    - canonical_map  : dict[tag → canonical_id]
        Résout n'importe quel tag vers son identifiant canonique.
        Exemple : canonical_map["fr:sucre"] = "en:sugar"

    - properties_map : dict[canonical_id → dict[propriété → valeur]]
        Propriétés associées à chaque entrée canonique.
        Exemple : properties_map["en:e471"] = {"additives_classes": "en:emulsifier"}
    """
    canonical_map: dict[str, str] = {}
    properties_map: dict[str, dict[str, str]] = {}

    current_canonical: str | None = None

    def _flush():
        """Réinitialise l'entrée courante (fin d'un bloc taxonomique)."""
        nonlocal current_canonical
        current_canonical = None

    def _to_tag(lang: str, value: str) -> str:
        """Crée un tag normalisé : 'en' + 'Sugar' → 'en:sugar'"""
        slug = _slugify(value)
        return f"{lang}:{slug}" if slug else ""

    def _ensure_props(canonical_id: str):
        """Initialise le dict de propriétés si nécessaire."""
        if canonical_id not in properties_map:
            properties_map[canonical_id] = {}

    for raw_line in text.splitlines():
        line = raw_line.strip()

        # Ligne vide ou commentaire → fin du bloc courant
        if not line or line.startswith("#"):
            _flush()
            continue

        # Stopwords et synonymes → ignorés
        if line.startswith(("stopwords:", "synonyms:")):
            continue

        # Relation parent : < en:sweetener
        if line.startswith("< "):
            if current_canonical:
                parent = line[2:].strip().lower()
                if parent:
                    _ensure_props(current_canonical)
                    existing = properties_map[current_canonical].get("parents", "")
                    if existing:
                        properties_map[current_canonical]["parents"] = existing + "," + parent
                    else:
                        properties_map[current_canonical]["parents"] = parent
            continue

        if ":" not in line:
            continue

        key, rest = line.split(":", 1)
        key = key.strip()
        rest = rest.strip()

        # Entrée de langue (en, fr, de, etc.)
        if _is_language_code(key):
            values = [v.strip() for v in rest.split(",") if v.strip()]
            if not values:
                continue

            # La première entrée "en:" d'un bloc devient le canonical_id
            if current_canonical is None and key == "en":
                canonical = _to_tag(key, values[0])
                if canonical:
                    current_canonical = canonical
                    canonical_map[canonical] = canonical
                    _ensure_props(canonical)

            # Toutes les variantes sont mappées vers le canonical
            if current_canonical:
                for val in values:
                    tag = _to_tag(key, val)
                    if tag:
                        canonical_map.setdefault(tag, current_canonical)
            continue

        # Propriété OFF (additives_classes, parents, etc.)
        if current_canonical is None:
            continue

        _ensure_props(current_canonical)
        properties_map[current_canonical][key] = rest

    logger.info(
        "Parsed taxonomy: %s canonical ids, %s known tags, %s property entries",
        sum(1 for k, v in canonical_map.items() if k == v),
        len(canonical_map),
        len(properties_map),
    )
    return canonical_map, properties_map


# ===========================================================================
# RÔLE DES ADDITIFS — Construction du mapping additif → rôle
# ===========================================================================

def _split_off_values(value: str | None) -> list[str]:
    """
    Sépare les valeurs d'une propriété OFF et nettoie le préfixe de langue.

    Entrée  : "en: en:emulsifier, en:stabiliser"
    Résultat : ["en:emulsifier", "en:stabiliser"]
    """
    if not value:
        return []
    value = re.sub(r'^[a-z]{2,3}:\s*', '', value.strip())
    parts = [part.strip().lower() for part in value.split(",") if part.strip()]
    return parts


def _build_additive_role_map(
    additives_props: dict[str, dict[str, str]],
    additive_classes_canonical_map: dict[str, str],
) -> dict[str, str]:
    """
    Construit le mapping : tag_additif → role (en texte lisible).

    Exemple : "en:e471" → "emulsifier"
    Exemple : "en:e330" → "acidity regulator"
    """
    role_map: dict[str, str] = {}

    for additive_id, props in additives_props.items():
        raw_classes = props.get("additives_classes")
        if not raw_classes:
            continue

        class_tags = _split_off_values(raw_classes)
        if not class_tags:
            continue

        resolved_roles = []
        for class_tag in class_tags:
            canonical_class_id = additive_classes_canonical_map.get(class_tag, class_tag)
            role_name = _id_to_name(canonical_class_id)
            if role_name and role_name not in resolved_roles:
                resolved_roles.append(role_name)

        # Garder uniquement le premier rôle (le plus significatif)
        if resolved_roles:
            role_map[additive_id.lower()] = resolved_roles[0]

    logger.info("Built additive role map for %s additive ids", len(role_map))
    return role_map


def _infer_role_off_only(tag_id: str, additive_role_map: dict[str, str]) -> str | None:
    """
    Cherche le rôle d'un ingrédient dans le role_map.
    Retourne None si l'ingrédient n'est pas un additif.
    """
    return additive_role_map.get((tag_id or "").lower())


# ===========================================================================
# NORMALISATION D'UN INGRÉDIENT — Résolution du tag canonique
# ===========================================================================

def _candidate_tags_from_item(item: dict) -> list[str]:
    """
    Génère les tags candidats pour chercher un ingrédient dans la taxonomie.

    Ordre de priorité :
    1. Le champ "id" tel quel (ex: "en:sugar")
    2. Le slug du "text" préfixé "en:" (ex: "en:sugar")
    3. Le slug du "text" préfixé "fr:" (ex: "fr:sucre")
    """
    candidates = []

    raw_id = item.get("id")
    if isinstance(raw_id, str) and raw_id.strip():
        candidates.append(raw_id.strip().lower())

    raw_text = _safe_text(item.get("text"))
    if raw_text:
        slug = _slugify(raw_text)
        if slug:
            candidates.append(f"en:{slug}")
            candidates.append(f"fr:{slug}")

    # Dédoublonner tout en gardant l'ordre
    unique = []
    seen = set()
    for c in candidates:
        if c and c not in seen:
            seen.add(c)
            unique.append(c)
    return unique


def _normalize_ingredient(
    item: dict,
    canonical_map: dict[str, str],
) -> dict | None:
    """
    Résout le tag canonique d'un item JSON.

    3 niveaux de fallback :
    1. TAXONOMIE — cherche les tags candidats dans canonical_map
    2. ID SOURCE — utilise le champ "id" du JSON tel quel
    3. TEXTE SOURCE — génère un tag depuis le champ "text"

    Retourne : {"tag_id": str, "raw_text": str | None} ou None
    """
    if not isinstance(item, dict):
        return None

    raw_text = _safe_text(item.get("text"))

    # 1. Tentative via la taxonomie OFF
    for candidate in _candidate_tags_from_item(item):
        if candidate in canonical_map:
            tag_id = canonical_map[candidate]
            return {"tag_id": tag_id, "raw_text": raw_text}

    # 2. Fallback sur l'id source
    raw_id = item.get("id")
    if isinstance(raw_id, str) and raw_id.strip():
        tag_id = raw_id.strip().lower()
        return {"tag_id": tag_id, "raw_text": raw_text}

    # 3. Fallback sur le texte source
    if raw_text:
        slug = _slugify(raw_text)
        if slug:
            tag_id = f"en:{slug}"
            return {"tag_id": tag_id, "raw_text": raw_text}

    return None


# ===========================================================================
# MAPPING TAG → ID STABLE
# ===========================================================================

def _build_id_mapping(
    product_rows: list[dict],
    component_rows: list[dict],
    alias_rows: list[dict],
) -> dict[str, int]:
    """
    Construit le mapping global : tag OFF → id stable (hash MD5 64 bits).

    Collecte tous les tags uniques présents dans les 3 tables et assigne
    à chacun un ID stable par hash MD5 (mod 2^63), cohérent entre
    initial load et tous les deltas futurs.

    Exemple :
        "en:butter"        → 3521863...
        "en:coconut-cream" → 7842194...
        "en:sugar"         → 1290384...

    Ce mapping est utilisé pour remplacer les tags textuels par des
    entiers dans toutes les tables de sortie.
    """
    all_tags: set[str] = set()

    for row in product_rows:
        if row.get("tag_id"):
            all_tags.add(row["tag_id"])

    for row in component_rows:
        if row.get("parent_tag_id"):
            all_tags.add(row["parent_tag_id"])
        if row.get("sous_tag_id"):
            all_tags.add(row["sous_tag_id"])

    for row in alias_rows:
        if row.get("tag_id"):
            all_tags.add(row["tag_id"])

    tag_to_id = {tag: _stable_id(tag) for tag in all_tags}

    logger.info(f"Built id mapping: {len(tag_to_id)} unique tags → stable numeric ids")

    return tag_to_id


# ===========================================================================
# FLATTEN — Aplatissement récursif de l'arbre d'ingrédients
# ===========================================================================

def _flatten_tree(
    code: Any,
    ingredients: Any,
    ingredients_canonical_map: dict[str, str],
    additive_role_map: dict[str, str],
    all_product_rows: list[dict],
    all_component_rows: list[dict],
    all_alias_rows: list[dict],
    parent_tag_id: str | None = None,
) -> None:
    """
    Parcourt récursivement l'arbre d'ingrédients d'un produit.

    En interne, les tags OFF (ex: "en:sugar") sont utilisés pour
    la résolution. Les ids stables sont assignés après le flatten.

    Dispatch selon le niveau :
    - parent_tag_id is None → niveau 1 → product_ingredients
    - parent_tag_id is not None → niveau 2+ → sous_ingredients
    """
    parsed = _parse_ingredients_value(ingredients)
    if not parsed:
        return

    for order, item in enumerate(parsed, start=1):
        normalized = _normalize_ingredient(item, ingredients_canonical_map)
        if not normalized:
            continue

        tag_id = normalized["tag_id"]
        raw_text = normalized["raw_text"]

        # ── Dispatch selon le niveau ──
        if parent_tag_id is None:
            # NIVEAU 1 → product_ingredients
            role = _infer_role_off_only(tag_id, additive_role_map)
            all_product_rows.append(
                {
                    "code": code,
                    "tag_id": tag_id,
                    "ingredient_order": order,
                    "role": role,
                }
            )
        else:
            # NIVEAU 2+ → sous_ingredients
            all_component_rows.append(
                {
                    "parent_tag_id": parent_tag_id,
                    "sous_tag_id": tag_id,
                    "rang": order,
                }
            )

        # ── Alias ──
        # Si le texte brut diffère du nom dérivé du tag, c'est un alias
        ingredient_name = _id_to_name(tag_id)
        alias_name = _normalize_alias_text(raw_text)
        if alias_name and alias_name != ingredient_name:
            all_alias_rows.append(
                {
                    "tag_id": tag_id,
                    "alias_name": alias_name,
                }
            )

        # ── Récursion sur les sous-ingrédients ──
        children = item.get("ingredients")
        if children:
            _flatten_tree(
                code=code,
                ingredients=children,
                ingredients_canonical_map=ingredients_canonical_map,
                additive_role_map=additive_role_map,
                all_product_rows=all_product_rows,
                all_component_rows=all_component_rows,
                all_alias_rows=all_alias_rows,
                parent_tag_id=tag_id,
            )


# ===========================================================================
# CONSTRUCTION DES TABLES FINALES (avec ids stables)
# ===========================================================================

def _build_ingredients_df(tag_to_id: dict[str, int]) -> pd.DataFrame:
    """
    Construit la table ingredients (référentiel).

    Colonnes :
        ingredient_id   : ID stable hash MD5 (PK)
        ingredient_name : nom lisible dérivé du tag

    Exemple :
        3521863... | butter
        7842194... | coconut cream
        1290384... | e150a
    """
    records = [
        {
            "ingredient_id": numeric_id,
            "ingredient_name": _id_to_name(tag),
        }
        for tag, numeric_id in sorted(tag_to_id.items(), key=lambda x: x[1])
    ]
    df = pd.DataFrame(records, columns=["ingredient_id", "ingredient_name"])
    df["ingredient_id"] = df["ingredient_id"].astype(pd.Int64Dtype())
    return df


def _build_product_ingredients_df(
    product_rows: list[dict],
    tag_to_id: dict[str, int],
) -> pd.DataFrame:
    """
    Construit la table product_ingredients.

    Remplace tag_id par l'id stable correspondant.

    Colonnes :
        code             : code-barres du produit
        ingredient_id    : ID stable (FK → ingredients)
        ingredient_order : position dans la liste (1 = le plus abondant)
        role             : rôle de l'additif (None si pas un additif)
    """
    records = [
        {
            "code": row["code"],
            "ingredient_id": tag_to_id[row["tag_id"]],
            "ingredient_order": row["ingredient_order"],
            "role": row["role"],
        }
        for row in product_rows
        if row.get("tag_id") in tag_to_id
    ]
    df = pd.DataFrame(records, columns=["code", "ingredient_id", "ingredient_order", "role"])
    if not df.empty:
        df["ingredient_id"] = df["ingredient_id"].astype(pd.Int64Dtype())
    return df


def _build_sous_ingredients_df(
    component_rows: list[dict],
    tag_to_id: dict[str, int],
) -> pd.DataFrame:
    """
    Construit la table sous_ingredients.

    Remplace parent_tag_id et sous_tag_id par les ids stables.

    Colonnes :
        ingredient_id        : ID stable (FK → ingredients, le parent)
        sous_ingredient_id   : ID stable (FK → ingredients, l'enfant)
        sous_ingredient_name : nom lisible du sous-ingrédient
        rang                 : position dans la sous-liste du parent
    """
    records = [
        {
            "ingredient_id": tag_to_id[row["parent_tag_id"]],
            "sous_ingredient_id": tag_to_id[row["sous_tag_id"]],
            "sous_ingredient_name": _id_to_name(row["sous_tag_id"]),
            "rang": row["rang"],
        }
        for row in component_rows
        if row.get("parent_tag_id") in tag_to_id and row.get("sous_tag_id") in tag_to_id
    ]
    df = pd.DataFrame(
        records,
        columns=["ingredient_id", "sous_ingredient_id", "sous_ingredient_name", "rang"],
    )
    if not df.empty:
        df["ingredient_id"] = df["ingredient_id"].astype(pd.Int64Dtype())
        df["sous_ingredient_id"] = df["sous_ingredient_id"].astype(pd.Int64Dtype())
    return df


def _build_alias_df(
    alias_rows: list[dict],
    tag_to_id: dict[str, int],
) -> pd.DataFrame:
    """
    Construit la table ingredient_alias.

    Remplace tag_id par l'id stable correspondant.

    Colonnes :
        ingredient_id : ID stable (FK → ingredients)
        alias_name    : variante textuelle de l'ingrédient
    """
    records = [
        {
            "ingredient_id": tag_to_id[row["tag_id"]],
            "alias_name": row["alias_name"],
        }
        for row in alias_rows
        if row.get("tag_id") in tag_to_id
    ]
    df = pd.DataFrame(records, columns=["ingredient_id", "alias_name"])
    if not df.empty:
        df["ingredient_id"] = df["ingredient_id"].astype(pd.Int64Dtype())
    return df


# ── DataFrames vides (utilisés quand aucune donnée n'est disponible) ──

def _empty_ingredients_df() -> pd.DataFrame:
    return pd.DataFrame({
        "ingredient_id": pd.array([], dtype=pd.Int64Dtype()),
        "ingredient_name": pd.array([], dtype="string"),
    })


def _empty_product_ingredients_df() -> pd.DataFrame:
    return pd.DataFrame({
        "code": pd.array([], dtype="string"),
        "ingredient_id": pd.array([], dtype=pd.Int64Dtype()),
        "ingredient_order": pd.array([], dtype=pd.Int32Dtype()),
        "role": pd.array([], dtype="string"),
    })


def _empty_sous_ingredients_df() -> pd.DataFrame:
    return pd.DataFrame({
        "ingredient_id": pd.array([], dtype=pd.Int64Dtype()),
        "sous_ingredient_id": pd.array([], dtype=pd.Int64Dtype()),
        "sous_ingredient_name": pd.array([], dtype="string"),
        "rang": pd.array([], dtype=pd.Int32Dtype()),
    })


def _empty_alias_df() -> pd.DataFrame:
    return pd.DataFrame({
        "ingredient_id": pd.array([], dtype=pd.Int64Dtype()),
        "alias_name": pd.array([], dtype="string"),
    })


# ===========================================================================
# POINT D'ENTRÉE PRINCIPAL
# ===========================================================================

def handle(
    input_file_key: str,
    ingredients_output_key: str,
    product_ingredients_output_key: str,
    sous_ingredients_output_key: str,
    ingredient_alias_output_key: str,
) -> None:
    """
    Orchestre la normalisation complète des ingrédients.

    Pipeline :
    1. Charger le parquet transformé depuis S3
    2. Charger et parser les 3 taxonomies OFF
    3. Construire le role map (additif → rôle)
    4. Aplatir l'arbre d'ingrédients de chaque produit (flatten)
    5. Construire le mapping tag → id stable (hash MD5)
    6. Construire les 4 DataFrames de sortie avec ids stables
    7. Uploader les 4 fichiers parquet sur S3
    """

    # ── Configuration S3 ──
    s3_bucket = os.environ["S3_BUCKET"]
    s3_endpoint = os.environ["S3_ENDPOINT"]
    s3_access_key = os.environ["S3_ACCESS_KEY"]
    s3_secret_key = os.environ["S3_SECRET_KEY"]

    logger.info(f"Normalizing ingredients from {input_file_key}...")

    s3_handler = S3FileHandler(
        s3_bucket,
        s3_endpoint,
        s3_access_key,
        s3_secret_key,
    )

    # ── Étape 1 : Charger le parquet transformé ──
    raw = s3_handler.download_to_memory(input_file_key)
    df = pd.read_parquet(raw)

    if "ingredients" not in df.columns:
        logger.error(
            "COLUMN 'ingredients' NOT FOUND IN PARQUET. "
            f"Available columns: {df.columns.tolist()}"
        )
        s3_handler.upload_dataframe(_empty_ingredients_df(), ingredients_output_key)
        s3_handler.upload_dataframe(_empty_product_ingredients_df(), product_ingredients_output_key)
        s3_handler.upload_dataframe(_empty_sous_ingredients_df(), sous_ingredients_output_key)
        s3_handler.upload_dataframe(_empty_alias_df(), ingredient_alias_output_key)
        return

    sample = df["ingredients"].dropna().head(3)
    logger.info(
        f"[DIAG] ingredients — dtype={df['ingredients'].dtype} | "
        f"non-null={df['ingredients'].notna().sum()}/{len(df)} | "
        f"sample types={[type(v).__name__ for v in sample]} | "
        f"sample values={[repr(v)[:120] for v in sample]}"
    )

    # ── Étape 2 : Charger et parser les taxonomies OFF ──
    ingredients_txt = _download_taxonomy(INGREDIENTS_TXT_URL)
    additives_txt = _download_taxonomy(ADDITIVES_TXT_URL)
    additive_classes_txt = _download_taxonomy(ADDITIVE_CLASSES_TXT_URL)

    ingredients_canonical_map, _ = _parse_taxonomy_with_properties(ingredients_txt)
    _, additives_props = _parse_taxonomy_with_properties(additives_txt)
    additive_classes_canonical_map, _ = _parse_taxonomy_with_properties(additive_classes_txt)

    # ── Étape 3 : Construire le role map ──
    additive_role_map = _build_additive_role_map(
        additives_props=additives_props,
        additive_classes_canonical_map=additive_classes_canonical_map,
    )

    # ── Étape 4 : Aplatir l'arbre d'ingrédients ──
    # En interne, les listes utilisent les tags OFF (ex: "en:sugar").
    # Les ids stables seront assignés à l'étape 5.
    all_product_rows: list[dict] = []
    all_component_rows: list[dict] = []
    all_alias_rows: list[dict] = []

    for _, row in df[["code", "ingredients"]].iterrows():
        _flatten_tree(
            code=row["code"],
            ingredients=row["ingredients"],
            ingredients_canonical_map=ingredients_canonical_map,
            additive_role_map=additive_role_map,
            all_product_rows=all_product_rows,
            all_component_rows=all_component_rows,
            all_alias_rows=all_alias_rows,
            parent_tag_id=None,
        )

    logger.info(f"Flattened product ingredient rows: {len(all_product_rows)}")
    logger.info(f"Flattened sous_ingredients rows: {len(all_component_rows)}")
    logger.info(f"Flattened alias rows: {len(all_alias_rows)}")

    # ── Étape 5 : Construire le mapping tag → id stable ──
    tag_to_id = _build_id_mapping(
        product_rows=all_product_rows,
        component_rows=all_component_rows,
        alias_rows=all_alias_rows,
    )

    # Log quelques exemples du mapping
    for tag, numeric_id in list(tag_to_id.items())[:10]:
        logger.info(f"  {tag} → {numeric_id} ({_id_to_name(tag)})")

    # ── Étape 6a : ingredients (référentiel) ──
    df_ingredients = _build_ingredients_df(tag_to_id)
    if df_ingredients.empty:
        df_ingredients = _empty_ingredients_df()

    # ── Étape 6b : product_ingredients ──
    df_product_ingredients = _build_product_ingredients_df(all_product_rows, tag_to_id)
    if not df_product_ingredients.empty:
        df_product_ingredients = (
            df_product_ingredients
            .drop_duplicates()
            .sort_values(["code", "ingredient_order", "ingredient_id"])
            .reset_index(drop=True)
        )
    else:
        df_product_ingredients = _empty_product_ingredients_df()

    # ── Étape 6c : sous_ingredients ──
    df_sous_ingredients = _build_sous_ingredients_df(all_component_rows, tag_to_id)
    if not df_sous_ingredients.empty:
        df_sous_ingredients = (
            df_sous_ingredients
            .drop_duplicates()
            .sort_values(["ingredient_id", "rang", "sous_ingredient_id"])
            .reset_index(drop=True)
        )
    else:
        df_sous_ingredients = _empty_sous_ingredients_df()

    # ── Étape 6d : ingredient_alias ──
    df_alias = _build_alias_df(all_alias_rows, tag_to_id)
    if not df_alias.empty:
        df_alias = (
            df_alias
            .drop_duplicates()
            .sort_values(["ingredient_id", "alias_name"])
            .reset_index(drop=True)
        )
    else:
        df_alias = _empty_alias_df()

    # ── Logs de diagnostic ──
    logger.info(f"df_ingredients shape: {df_ingredients.shape}")
    logger.info(f"df_product_ingredients shape: {df_product_ingredients.shape}")
    logger.info(f"df_sous_ingredients shape: {df_sous_ingredients.shape}")
    logger.info(f"df_alias shape: {df_alias.shape}")

    if not df_ingredients.empty:
        logger.info("df_ingredients head:\n" + df_ingredients.head(10).to_string())

    if not df_product_ingredients.empty:
        logger.info("df_product_ingredients head:\n" + df_product_ingredients.head(10).to_string())

    if not df_sous_ingredients.empty:
        logger.info("df_sous_ingredients head:\n" + df_sous_ingredients.head(10).to_string())

    if not df_alias.empty:
        logger.info("df_alias head:\n" + df_alias.head(10).to_string())

    # ── Étape 7 : Upload des 4 fichiers parquet sur S3 ──
    s3_handler.upload_dataframe(df_ingredients, ingredients_output_key)
    logger.info(f"ingredients uploaded -> {ingredients_output_key} ({len(df_ingredients)} records)")

    s3_handler.upload_dataframe(df_product_ingredients, product_ingredients_output_key)
    logger.info(
        f"product_ingredients uploaded -> {product_ingredients_output_key} "
        f"({len(df_product_ingredients)} records)"
    )

    s3_handler.upload_dataframe(df_sous_ingredients, sous_ingredients_output_key)
    logger.info(
        f"sous_ingredients uploaded -> {sous_ingredients_output_key} "
        f"({len(df_sous_ingredients)} records)"
    )

    s3_handler.upload_dataframe(df_alias, ingredient_alias_output_key)
    logger.info(
        f"ingredient_alias uploaded -> {ingredient_alias_output_key} "
        f"({len(df_alias)} records)"
    )
