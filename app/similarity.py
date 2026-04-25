import json
import logging
import math
import re
import unicodedata
from collections import Counter
from threading import Lock
from typing import Any, Dict, List, Optional, Sequence

from models import (
    CATEGORIES_TABLE,
    PRODUCT_INGREDIENTS_TABLE,
    clean_name,
    execute_query,
    get_ancestor_categories_table,
    get_category_links_sql,
    get_ingredient_alias_table,
    get_product_by_code,
    get_products_by_codes,
    get_products_list,
)
from openai_utils import (
    OpenAIUnavailableError,
    get_structured_json_response,
    get_text_embeddings,
    is_openai_available,
)
from config import OPENAI_NORMALIZATION_MODEL

logger = logging.getLogger(__name__)

NUTRITION_VECTOR_KEYS = [
    "sugars_100g",
    "carbohydrates_100g",
    "fat_100g",
    "proteins_100g",
    "salt_100g",
    "fiber_100g",
]

NUTRISCORE_ORDER = {
    "a": 5,
    "b": 4,
    "c": 3,
    "d": 2,
    "e": 1,
}

STOPWORDS = {
    "a",
    "al",
    "and",
    "au",
    "aux",
    "avec",
    "bio",
    "canada",
    "canadian",
    "dans",
    "de",
    "des",
    "du",
    "en",
    "et",
    "for",
    "free",
    "from",
    "la",
    "le",
    "les",
    "of",
    "organic",
    "partly",
    "pour",
    "sans",
    "skim",
    "the",
    "to",
    "with",
}

LLM_NORMALIZATION_BATCH_SIZE = 12
_NORMALIZED_TERMS_CACHE: Dict[str, List[str]] = {}
_NORMALIZED_TERMS_CACHE_LOCK = Lock()


def normalize_text(value: object) -> str:
    text = str(value or "").lower().strip()
    if not text:
        return ""

    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"^[a-z]{2,3}:", "", text)
    text = text.replace("-", " ")
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def tokenize_text(value: object) -> List[str]:
    tokens = normalize_text(value).split()
    return [
        token
        for token in tokens
        if len(token) > 1 and not token.isdigit() and token not in STOPWORDS
    ]


def cosine_similarity_dense(left: Sequence[float], right: Sequence[float]) -> float:
    if not left or not right:
        return 0.0

    numerator = sum(a * b for a, b in zip(left, right))
    left_norm = math.sqrt(sum(a * a for a in left))
    right_norm = math.sqrt(sum(b * b for b in right))
    if not left_norm or not right_norm:
        return 0.0
    return numerator / (left_norm * right_norm)


def cosine_similarity_sparse(left: Dict[str, float], right: Dict[str, float]) -> float:
    if not left or not right:
        return 0.0

    common = set(left).intersection(right)
    numerator = sum(left[token] * right[token] for token in common)
    left_norm = math.sqrt(sum(value * value for value in left.values()))
    right_norm = math.sqrt(sum(value * value for value in right.values()))
    if not left_norm or not right_norm:
        return 0.0
    return numerator / (left_norm * right_norm)


def euclidean_similarity_dense(left: Sequence[float], right: Sequence[float]) -> float:
    if not left or not right:
        return 0.0

    distance = math.sqrt(sum((a - b) * (a - b) for a, b in zip(left, right)))
    left_norm = math.sqrt(sum(a * a for a in left))
    right_norm = math.sqrt(sum(b * b for b in right))
    scale = left_norm + right_norm
    if not scale:
        return 0.0

    # Normalise la distance euclidienne pour obtenir un score interpretable entre 0 et 1.
    return max(0.0, 1.0 - (distance / scale))


def get_category_labels(product: Optional[Dict]) -> List[str]:
    labels: List[str] = []
    if not product:
        return labels

    for cat in product.get("categories", []):
        if isinstance(cat, str):
            label = cat.strip()
        elif isinstance(cat, dict):
            label = str(cat.get("child") or cat.get("display") or "").strip()
        else:
            label = ""

        if label:
            labels.append(label)

    return labels


def get_parent_category_labels(product: Optional[Dict]) -> List[str]:
    labels: List[str] = []
    if not product:
        return labels

    for cat in product.get("categories", []):
        if isinstance(cat, dict):
            label = str(cat.get("parent") or "").strip()
            if label:
                labels.append(label)
    return labels


def get_target_category(product: Optional[Dict]) -> str:
    labels = get_category_labels(product)
    if not labels:
        return ""

    return sorted(
        labels,
        key=lambda value: (len(value.split()), len(value)),
        reverse=True,
    )[0]


def get_top_ingredients(product: Optional[Dict], max_items: int = 8) -> List[str]:
    seen = set()
    out: List[str] = []

    for ingredient in (product or {}).get("ingredients", []):
        clean = str(ingredient or "").strip()
        key = normalize_text(clean)
        if not clean or not key or key in seen:
            continue
        seen.add(key)
        out.append(clean)
        if len(out) >= max_items:
            break

    return out


def normalize_numeric_feature(value: object) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0.0

    if not math.isfinite(numeric):
        return 0.0

    return math.log1p(max(numeric, 0.0))


def get_nutriscore_value(product: Optional[Dict]) -> float:
    grade = str((product or {}).get("nutriscore_grade") or "").strip().lower()
    return float(NUTRISCORE_ORDER.get(grade, 0.0))


def build_nutrition_vector(product: Optional[Dict]) -> List[float]:
    product = product or {}
    vector = [normalize_numeric_feature(product.get(key)) for key in NUTRITION_VECTOR_KEYS]
    vector.append(get_nutriscore_value(product) / 5.0)
    return vector


def get_product_aliases_map(codes: Sequence[str], max_items: int = 8) -> Dict[str, List[str]]:
    ordered_codes = list(dict.fromkeys(str(code).strip() for code in codes if str(code).strip()))
    alias_map: Dict[str, List[str]] = {code: [] for code in ordered_codes}
    if not ordered_codes:
        return alias_map

    alias_table = get_ingredient_alias_table()
    if not alias_table:
        return alias_map

    placeholders = ", ".join(["?"] * len(ordered_codes))
    rows = execute_query(
        f"""
        SELECT pi.code, ia.alias_name
        FROM {PRODUCT_INGREDIENTS_TABLE} pi
        JOIN {alias_table} ia ON ia.ingredient_id = pi.ingredient_id
        WHERE pi.code IN ({placeholders})
          AND ia.alias_name IS NOT NULL
          AND trim(ia.alias_name) <> ''
        ORDER BY pi.code, pi.ingredient_order NULLS LAST, ia.alias_name
        """,
        ordered_codes,
    )

    seen_by_code = {code: set() for code in ordered_codes}
    for row in rows:
        product_code = str(row.get("code") or "").strip()
        if product_code not in alias_map or len(alias_map[product_code]) >= max_items:
            continue

        alias_name = clean_name(str(row.get("alias_name") or "").strip())
        alias_key = normalize_text(alias_name)
        if not alias_name or not alias_key or alias_key in seen_by_code[product_code]:
            continue

        seen_by_code[product_code].add(alias_key)
        alias_map[product_code].append(alias_name)

    return alias_map


def batch_items(items: Sequence[Any], size: int) -> List[List[Any]]:
    if size <= 0:
        return [list(items)]
    return [list(items[index : index + size]) for index in range(0, len(items), size)]


def dedupe_terms(values: Sequence[str], max_items: int = 8) -> List[str]:
    out: List[str] = []
    seen = set()

    for value in values:
        clean = clean_name(str(value or "").strip())
        key = normalize_text(clean)
        if not clean or not key or key in seen:
            continue
        seen.add(key)
        out.append(clean)
        if len(out) >= max_items:
            break

    return out


def get_llm_normalized_terms_by_code(
    products: Sequence[Dict],
    alias_map: Dict[str, List[str]],
    max_items: int = 8,
) -> Dict[str, List[str]]:
    if not is_openai_available():
        return {}

    payload_items: List[Dict[str, Any]] = []
    cache_keys_by_code: Dict[str, str] = {}
    normalized_by_code: Dict[str, List[str]] = {}
    for product in products:
        product_code = str(product.get("code") or "").strip()
        if not product_code:
            continue
        raw_terms = [
            *get_top_ingredients(product, max_items=max_items),
            *alias_map.get(product_code, [])[:max_items],
        ]
        payload_item = {
            "code": product_code,
            "product_name": str(product.get("product_name") or "").strip(),
            "category": get_target_category(product),
            "terms": dedupe_terms(raw_terms, max_items=max_items * 2),
        }
        cache_key = json.dumps(
            {
                "model": OPENAI_NORMALIZATION_MODEL,
                "code": payload_item["code"],
                "product_name": payload_item["product_name"],
                "category": payload_item["category"],
                "terms": payload_item["terms"],
                "max_items": max_items,
            },
            ensure_ascii=False,
        )
        cache_keys_by_code[product_code] = cache_key

        with _NORMALIZED_TERMS_CACHE_LOCK:
            cached_terms = _NORMALIZED_TERMS_CACHE.get(cache_key)

        if cached_terms is not None:
            normalized_by_code[product_code] = list(cached_terms)
            continue

        payload_items.append(payload_item)

    if not payload_items:
        return normalized_by_code

    schema = {
        "type": "object",
        "properties": {
            "products": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "code": {"type": "string"},
                        "normalized_terms": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                    },
                    "required": ["code", "normalized_terms"],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["products"],
        "additionalProperties": False,
    }

    for chunk in batch_items(payload_items, LLM_NORMALIZATION_BATCH_SIZE):
        try:
            response = get_structured_json_response(
                instructions=(
                    "Normalize product ingredient and alias terms. "
                    "Merge close aliases into canonical ingredient labels, remove duplicates, "
                    "remove packaging/noise terms, and keep at most 8 meaningful ingredients per product."
                ),
                user_input=json.dumps({"products": chunk}, ensure_ascii=False),
                schema_name="normalized_product_terms",
                schema=schema,
                model=OPENAI_NORMALIZATION_MODEL,
            )
        except OpenAIUnavailableError as exc:
            logger.warning("LLM ingredient normalization disabled for this run: %s", exc)
            return normalized_by_code

        for item in response.get("products", []):
            if not isinstance(item, dict):
                continue
            product_code = str(item.get("code") or "").strip()
            if not product_code:
                continue
            normalized_terms = dedupe_terms(item.get("normalized_terms", []), max_items=max_items)
            if normalized_terms:
                normalized_by_code[product_code] = normalized_terms
                cache_key = cache_keys_by_code.get(product_code)
                if cache_key:
                    with _NORMALIZED_TERMS_CACHE_LOCK:
                        _NORMALIZED_TERMS_CACHE[cache_key] = list(normalized_terms)

    return normalized_by_code


def build_semantic_text(
    product: Dict,
    aliases: Sequence[str],
    normalized_terms: Optional[Sequence[str]] = None,
) -> str:
    name = str(product.get("product_name") or "").strip()
    category = get_target_category(product)
    ingredients = ", ".join(get_top_ingredients(product, max_items=8))
    normalized_ingredients = ", ".join(dedupe_terms(normalized_terms or [], max_items=8))
    alias_text = ", ".join(
        list(
            dict.fromkeys(
                str(alias).strip()
                for alias in aliases
                if str(alias).strip()
            )
        )[:8]
    )

    lines = []
    if name:
        lines.append(f"Nom: {name}")
    if category:
        lines.append(f"Categorie: {category}")
    if ingredients:
        lines.append(f"Ingredients: {ingredients}")
    if normalized_ingredients:
        lines.append(f"Ingredients normalises: {normalized_ingredients}")
    if alias_text:
        lines.append(f"Alias: {alias_text}")
    return "\n".join(lines)


def build_product_category_context(products: Sequence[Dict]) -> Dict[str, Dict[str, set[str]]]:
    ordered_codes = [
        str(product.get("code") or "").strip()
        for product in products
        if str(product.get("code") or "").strip()
    ]
    context: Dict[str, Dict[str, set[str]]] = {
        code: {"categories": set(), "parents": set(), "ancestors": set()}
        for code in ordered_codes
    }

    for product in products:
        product_code = str(product.get("code") or "").strip()
        if product_code not in context:
            continue

        for label in get_category_labels(product):
            normalized = normalize_text(label)
            if normalized:
                context[product_code]["categories"].add(normalized)

        for label in get_parent_category_labels(product):
            normalized = normalize_text(label)
            if normalized:
                context[product_code]["parents"].add(normalized)

    ancestor_categories_table = get_ancestor_categories_table()
    if not ancestor_categories_table or not ordered_codes:
        return context

    placeholders = ", ".join(["?"] * len(ordered_codes))
    rows = execute_query(
        f"""
        WITH category_links AS (
            {get_category_links_sql()}
        )
        SELECT DISTINCT cl.code, ancestor.category_name AS ancestor_name
        FROM category_links cl
        JOIN {ancestor_categories_table} a
          ON cl.category_id = a.category_id
        JOIN {CATEGORIES_TABLE} ancestor
          ON ancestor.category_id = a.category_id_parent
        WHERE cl.code IN ({placeholders})
          AND a.distance > 1
        """,
        ordered_codes,
    )

    for row in rows:
        product_code = str(row.get("code") or "").strip()
        if product_code not in context:
            continue

        normalized = normalize_text(clean_name(str(row.get("ancestor_name") or "").strip()))
        if normalized:
            context[product_code]["ancestors"].add(normalized)

    return context


def build_tfidf_vectors(texts: List[str]) -> List[Dict[str, float]]:
    token_lists = [tokenize_text(text) for text in texts]
    document_frequency: Counter[str] = Counter()

    for tokens in token_lists:
        for token in set(tokens):
            document_frequency[token] += 1

    total_docs = max(len(token_lists), 1)
    idf = {
        token: math.log((1 + total_docs) / (1 + freq)) + 1
        for token, freq in document_frequency.items()
    }

    vectors: List[Dict[str, float]] = []
    for tokens in token_lists:
        counts = Counter(tokens)
        total_tokens = sum(counts.values()) or 1
        vectors.append(
            {
                token: (count / total_tokens) * idf[token]
                for token, count in counts.items()
            }
        )

    return vectors


def compute_text_similarities(base_text: str, candidate_texts: List[str]) -> List[float]:
    if not base_text.strip():
        return [0.0] * len(candidate_texts)

    texts = [base_text, *candidate_texts]
    nonempty_indexes = [index for index, text in enumerate(texts) if str(text).strip()]
    if not nonempty_indexes or nonempty_indexes[0] != 0:
        return [0.0] * len(candidate_texts)

    dense_scores: List[Optional[float]] = [None] * len(candidate_texts)
    nonempty_texts = [texts[index] for index in nonempty_indexes]

    if is_openai_available():
        try:
            vectors = get_text_embeddings(nonempty_texts)
            base_vector = vectors[0]
            for local_index, global_index in enumerate(nonempty_indexes[1:], start=1):
                dense_scores[global_index - 1] = cosine_similarity_dense(
                    base_vector,
                    vectors[local_index],
                )
            return [float(score or 0.0) for score in dense_scores]
        except OpenAIUnavailableError as exc:
            logger.warning("Embedding fallback to local TF-IDF: %s", exc)

    sparse_vectors = build_tfidf_vectors(nonempty_texts)
    base_vector = sparse_vectors[0]
    for local_index, global_index in enumerate(nonempty_indexes[1:], start=1):
        dense_scores[global_index - 1] = cosine_similarity_sparse(
            base_vector,
            sparse_vectors[local_index],
        )
    return [float(score or 0.0) for score in dense_scores]


def get_nutrition_similarity(base_product: Dict, candidate_product: Dict) -> float:
    return euclidean_similarity_dense(
        build_nutrition_vector(base_product),
        build_nutrition_vector(candidate_product),
    )


def get_category_hierarchy_score(base_context: Dict, candidate_context: Dict) -> float:
    base_categories = set(base_context.get("categories", set()))
    candidate_categories = set(candidate_context.get("categories", set()))
    if base_categories.intersection(candidate_categories):
        return 1.0

    base_parents = set(base_context.get("parents", set()))
    candidate_parents = set(candidate_context.get("parents", set()))
    if base_parents.intersection(candidate_parents):
        return 0.7

    base_ancestors = set(base_context.get("ancestors", set()))
    candidate_ancestors = set(candidate_context.get("ancestors", set()))
    if base_ancestors.intersection(candidate_ancestors):
        return 0.4

    return 0.0


def get_seed_query(product: Dict) -> str:
    meaningful_tokens = tokenize_text(product.get("product_name"))
    if not meaningful_tokens:
        return ""
    return " ".join(meaningful_tokens[:2])


def merge_seed_candidates(groups: Sequence[List[Dict]], current_code: str, limit: int) -> List[str]:
    codes: List[str] = []
    seen = {current_code}

    for group in groups:
        for item in group:
            code = str(item.get("code") or "").strip()
            if not code or code in seen:
                continue
            seen.add(code)
            codes.append(code)
            if len(codes) >= limit:
                return codes

    return codes


def get_similar_products(
    code: str,
    limit: int = 4,
    candidate_pool: int = 20,
) -> Optional[List[Dict]]:
    base_product = get_product_by_code(code)
    if not base_product:
        return None

    target_category = get_target_category(base_product)
    base_brand = str(base_product.get("brands") or "").strip()
    seed_query = get_seed_query(base_product)

    seed_groups: List[List[Dict]] = []
    if target_category:
        seed_groups.append(get_products_list(category=target_category, limit=candidate_pool))
    if seed_query:
        seed_groups.append(get_products_list(q=seed_query, limit=candidate_pool))
    if base_brand:
        seed_groups.append(get_products_list(brand=base_brand, limit=candidate_pool))

    candidate_codes = merge_seed_candidates(seed_groups, str(code), candidate_pool)
    candidate_products = get_products_by_codes(candidate_codes)
    if not candidate_products:
        return []

    products = [base_product, *candidate_products]
    ordered_codes = [str(product.get("code") or "").strip() for product in products]
    alias_map = get_product_aliases_map(ordered_codes, max_items=8)
    normalized_terms_by_code = get_llm_normalized_terms_by_code(products, alias_map, max_items=8)
    semantic_texts = {
        str(product["code"]): build_semantic_text(
            product,
            alias_map.get(str(product["code"]), []),
            normalized_terms_by_code.get(str(product["code"]), []),
        )
        for product in products
    }
    category_context = build_product_category_context(products)
    base_code = str(base_product["code"])
    semantic_scores = compute_text_similarities(
        semantic_texts[base_code],
        [semantic_texts[str(product["code"])] for product in candidate_products],
    )

    ranked: List[Dict] = []
    for index, candidate_product in enumerate(candidate_products):
        candidate_code = str(candidate_product["code"])
        semantic_similarity = semantic_scores[index]
        nutrition_similarity = get_nutrition_similarity(base_product, candidate_product)
        category_similarity = get_category_hierarchy_score(
            category_context.get(base_code, {}),
            category_context.get(candidate_code, {}),
        )
        preliminary_similarity = (
            semantic_similarity
            + nutrition_similarity
            + category_similarity
        ) / 3.0

        enriched = dict(candidate_product)
        enriched["category_label"] = get_target_category(candidate_product)
        enriched["top_ingredients"] = get_top_ingredients(candidate_product, max_items=5)
        enriched["normalized_ingredients"] = get_top_ingredients(candidate_product, max_items=8)
        enriched["ingredient_roles"] = []
        enriched["alias_sources"] = {
            "product_aliases": len(alias_map.get(candidate_code, [])),
            "llm_normalized_terms": len(normalized_terms_by_code.get(candidate_code, [])),
        }
        enriched["role_sources"] = {}
        enriched["ingredient_similarity_pct"] = round(semantic_similarity * 100)
        enriched["nutriment_similarity_pct"] = round(nutrition_similarity * 100)
        enriched["category_similarity_pct"] = round(category_similarity * 100)
        enriched["overall_similarity_pct"] = round(preliminary_similarity * 100)
        enriched["similarity_score"] = round(preliminary_similarity, 4)
        ranked.append(enriched)

    ranked.sort(
        key=lambda item: (
            -float(item.get("similarity_score") or 0),
            str(item.get("product_name") or ""),
        )
    )

    return ranked[:limit]
