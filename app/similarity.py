import logging
import math
import re
import unicodedata
from collections import Counter
from functools import lru_cache
from typing import Dict, List, Optional, Sequence

from alias_cache import get_alias_cache_entries, save_alias_cache_entries
from models import (
    INGREDIENTS_TABLE,
    clean_name,
    execute_query,
    get_ingredient_alias_table,
    get_product_by_code,
    get_products_by_codes,
    get_products_list,
)
from openai_utils import OpenAIUnavailableError, get_text_embeddings, is_openai_available, normalize_aliases_with_llm

logger = logging.getLogger(__name__)

NUTRIENT_KEYS = [
    "energy_kcal_100g",
    "fat_100g",
    "sugars_100g",
    "salt_100g",
    "proteins_100g",
    "carbohydrates_100g",
    "fiber_100g",
    "saturated_fat_100g",
]

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

NOISE_TOKENS = {
    "calories",
    "contains",
    "daily",
    "free",
    "guaranteed",
    "ingredients",
    "less",
    "more",
    "nutrition",
    "percent",
    "per",
    "serving",
    "storage",
    "valeur",
    "quotidienne",
}


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
                dense_scores[global_index - 1] = cosine_similarity_dense(base_vector, vectors[local_index])
            return [float(score or 0.0) for score in dense_scores]
        except OpenAIUnavailableError as exc:
            logger.warning("Embedding fallback to local TF-IDF: %s", exc)

    sparse_vectors = build_tfidf_vectors(nonempty_texts)
    base_vector = sparse_vectors[0]
    for local_index, global_index in enumerate(nonempty_indexes[1:], start=1):
        dense_scores[global_index - 1] = cosine_similarity_sparse(base_vector, sparse_vectors[local_index])
    return [float(score or 0.0) for score in dense_scores]


@lru_cache(maxsize=1)
def get_exact_ingredient_map() -> Dict[str, str]:
    rows = execute_query(f"SELECT ingredient_name FROM {INGREDIENTS_TABLE}")
    mapping: Dict[str, str] = {}

    for row in rows:
        canonical = clean_name(row.get("ingredient_name"))
        key = normalize_text(canonical)
        if key and key not in mapping:
            mapping[key] = canonical

    return mapping


@lru_cache(maxsize=1)
def get_alias_candidates_map() -> Dict[str, List[str]]:
    alias_table = get_ingredient_alias_table()
    if not alias_table:
        return {}

    rows = execute_query(
        f"""
        SELECT ia.alias_name, i.ingredient_name
        FROM {alias_table} ia
        JOIN {INGREDIENTS_TABLE} i ON i.ingredient_id = ia.ingredient_id
        WHERE ia.alias_name IS NOT NULL
          AND trim(ia.alias_name) <> ''
          AND i.ingredient_name IS NOT NULL
          AND trim(i.ingredient_name) <> ''
        """
    )

    mapping: Dict[str, set] = {}
    for row in rows:
        alias_key = normalize_text(row.get("alias_name"))
        canonical = clean_name(row.get("ingredient_name"))
        if not alias_key or not canonical:
            continue
        mapping.setdefault(alias_key, set()).add(canonical)

    return {
        alias_key: sorted(values)
        for alias_key, values in mapping.items()
    }


def looks_like_noise(text: str) -> bool:
    tokens = text.split()
    if not tokens:
        return True
    if len(tokens) >= 8:
        return True
    if sum(token in NOISE_TOKENS for token in tokens) >= 2:
        return True
    if any(char.isdigit() for char in text) and len(tokens) >= 5:
        return True
    return False


def choose_fallback_canonical(raw: str, candidates: List[str]) -> Dict:
    if candidates:
        return {
            "canonical": sorted(candidates)[0],
            "source": "alias_table_fallback",
            "is_noise": False,
        }

    normalized = normalize_text(raw)
    if looks_like_noise(normalized):
        return {
            "canonical": "",
            "source": "heuristic_noise",
            "is_noise": True,
        }

    exact_map = get_exact_ingredient_map()
    if normalized in exact_map:
        return {
            "canonical": exact_map[normalized],
            "source": "exact_ingredient",
            "is_noise": False,
        }

    return {
        "canonical": clean_name(raw),
        "source": "heuristic_clean",
        "is_noise": False,
    }


def canonicalize_candidate_name(name: str, fallback_candidates: List[str]) -> str:
    cleaned = clean_name(name)
    key = normalize_text(cleaned)
    exact_map = get_exact_ingredient_map()
    if key in exact_map:
        return exact_map[key]

    for candidate in fallback_candidates:
        if normalize_text(candidate) == key:
            return candidate

    return cleaned


def resolve_ingredient_aliases(raw_values: Sequence[str]) -> Dict[str, Dict]:
    exact_map = get_exact_ingredient_map()
    alias_map = get_alias_candidates_map()
    results: Dict[str, Dict] = {}
    pending_llm: List[Dict[str, object]] = []
    unique_values = list(dict.fromkeys(str(value).strip() for value in raw_values if str(value).strip()))
    normalized_by_raw = {raw: normalize_text(raw) for raw in unique_values}
    cached_entries = get_alias_cache_entries(normalized_by_raw.values())

    for raw in unique_values:
        normalized = normalized_by_raw[raw]
        if not normalized:
            results[raw] = {"canonical": "", "source": "empty", "is_noise": True}
            continue

        if normalized in exact_map:
            results[raw] = {
                "canonical": exact_map[normalized],
                "source": "exact_ingredient",
                "is_noise": False,
            }
            continue

        cached = cached_entries.get(normalized)
        if cached:
            results[raw] = dict(cached)
            continue

        candidates = alias_map.get(normalized, [])
        if len(candidates) == 1:
            results[raw] = {
                "canonical": candidates[0],
                "source": "alias_table",
                "is_noise": False,
            }
            continue

        pending_llm.append(
            {
                "raw": raw,
                "candidates": candidates,
            }
        )

    if pending_llm and is_openai_available():
        try:
            llm_results = normalize_aliases_with_llm(pending_llm)
        except OpenAIUnavailableError as exc:
            logger.warning("LLM alias normalization disabled for this run: %s", exc)
            llm_results = {}

        for item in pending_llm:
            raw = str(item["raw"])
            if raw not in llm_results:
                continue

            llm_result = llm_results[raw]
            canonical = str(llm_result.get("canonical") or "").strip()
            if canonical:
                canonical = canonicalize_candidate_name(canonical, list(item.get("candidates", [])))
            results[raw] = {
                "canonical": canonical,
                "source": llm_result.get("source", "llm"),
                "is_noise": bool(llm_result.get("is_noise")) or not canonical,
            }

    cache_updates: Dict[str, Dict] = {}
    for item in pending_llm:
        raw = str(item["raw"])
        if raw in results:
            cache_updates[normalized_by_raw[raw]] = dict(results[raw])
            continue
        results[raw] = choose_fallback_canonical(raw, list(item.get("candidates", [])))
        cache_updates[normalized_by_raw[raw]] = dict(results[raw])

    save_alias_cache_entries(cache_updates)

    return results


def build_ingredient_profile(product: Dict, alias_resolutions: Dict[str, Dict]) -> Dict:
    raw_ingredients = get_top_ingredients(product, max_items=8)
    normalized_ingredients: List[str] = []
    seen = set()
    sources = Counter()

    for raw in raw_ingredients:
        resolution = alias_resolutions.get(raw) or choose_fallback_canonical(raw, [])
        canonical = str(resolution.get("canonical") or "").strip()
        if not canonical:
            continue

        key = normalize_text(canonical)
        if not key or key in seen:
            continue
        seen.add(key)
        normalized_ingredients.append(canonical)
        sources[str(resolution.get("source") or "unknown")] += 1

    return {
        "raw_ingredients": raw_ingredients,
        "normalized_ingredients": normalized_ingredients,
        "signature": ", ".join(normalized_ingredients),
        "sources": dict(sources),
    }


def get_category_signature(product: Dict) -> str:
    parts: List[str] = []
    for cat in product.get("categories", []):
        if isinstance(cat, str):
            label = cat.strip()
            if label:
                parts.append(label)
        elif isinstance(cat, dict):
            parent = str(cat.get("parent") or "").strip()
            child = str(cat.get("child") or "").strip()
            if child:
                parts.append(child)
            if parent and child:
                parts.append(f"{parent} {child}")
    return " | ".join(dict.fromkeys(parts))


def get_ingredient_overlap(base_profile: Dict, candidate_profile: Dict) -> float:
    left = {normalize_text(value) for value in base_profile.get("normalized_ingredients", [])}
    right = {normalize_text(value) for value in candidate_profile.get("normalized_ingredients", [])}
    left.discard("")
    right.discard("")

    if not left or not right:
        return 0.0

    intersection = len(left.intersection(right))
    union = len(left.union(right))
    return intersection / union if union else 0.0


def get_nutrition_similarity(base_product: Dict, candidate_product: Dict) -> float:
    total = 0.0
    count = 0

    for key in NUTRIENT_KEYS:
        try:
            left = float(base_product.get(key))
            right = float(candidate_product.get(key))
        except (TypeError, ValueError):
            continue

        reference = max(abs(left), abs(right), 1.0)
        score = max(0.0, 1.0 - (abs(left - right) / reference))
        total += score
        count += 1

    if not count:
        return 0.0
    return total / count


def get_category_hierarchy_score(base_product: Dict, candidate_product: Dict) -> float:
    base_target = normalize_text(get_target_category(base_product))
    if not base_target:
        return 0.0

    candidate_labels = {normalize_text(label) for label in get_category_labels(candidate_product)}
    if base_target in candidate_labels:
        return 1.0

    base_parents = {normalize_text(label) for label in get_parent_category_labels(base_product)}
    candidate_parents = {normalize_text(label) for label in get_parent_category_labels(candidate_product)}
    if base_parents.intersection(candidate_parents):
        return 0.7

    base_tokens = set(tokenize_text(get_category_signature(base_product)))
    candidate_tokens = set(tokenize_text(get_category_signature(candidate_product)))
    if not base_tokens or not candidate_tokens:
        return 0.0

    overlap = len(base_tokens.intersection(candidate_tokens)) / len(base_tokens.union(candidate_tokens))
    return overlap * 0.5


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
    candidate_pool: int = 40,
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
    all_raw_ingredients: List[str] = []
    for product in products:
        all_raw_ingredients.extend(get_top_ingredients(product, max_items=8))

    alias_resolutions = resolve_ingredient_aliases(all_raw_ingredients)
    ingredient_profiles = {
        str(product["code"]): build_ingredient_profile(product, alias_resolutions)
        for product in products
    }

    ingredient_signature_texts = [
        ingredient_profiles[str(product["code"])]["signature"]
        for product in candidate_products
    ]
    category_signature_texts = [
        get_category_signature(product)
        for product in candidate_products
    ]

    base_ingredient_signature = ingredient_profiles[str(base_product["code"])]["signature"]
    base_category_signature = get_category_signature(base_product)

    ingredient_vector_scores = compute_text_similarities(
        base_ingredient_signature,
        ingredient_signature_texts,
    )
    category_vector_scores = compute_text_similarities(
        base_category_signature,
        category_signature_texts,
    )

    ranked: List[Dict] = []
    for index, candidate_product in enumerate(candidate_products):
        candidate_profile = ingredient_profiles[str(candidate_product["code"])]
        ingredient_overlap = get_ingredient_overlap(
            ingredient_profiles[str(base_product["code"])],
            candidate_profile,
        )
        ingredient_similarity = max(
            ingredient_overlap,
            (0.65 * ingredient_vector_scores[index]) + (0.35 * ingredient_overlap),
        )
        nutrition_similarity = get_nutrition_similarity(base_product, candidate_product)
        category_similarity = max(
            category_vector_scores[index],
            get_category_hierarchy_score(base_product, candidate_product),
        )
        overall_similarity = (
            (0.45 * ingredient_similarity)
            + (0.35 * nutrition_similarity)
            + (0.20 * category_similarity)
        )

        enriched = dict(candidate_product)
        enriched["category_label"] = get_target_category(candidate_product)
        enriched["top_ingredients"] = candidate_profile["normalized_ingredients"][:5]
        enriched["normalized_ingredients"] = candidate_profile["normalized_ingredients"]
        enriched["alias_sources"] = candidate_profile["sources"]
        enriched["ingredient_similarity_pct"] = round(ingredient_similarity * 100)
        enriched["nutriment_similarity_pct"] = round(nutrition_similarity * 100)
        enriched["category_similarity_pct"] = round(category_similarity * 100)
        enriched["overall_similarity_pct"] = round(overall_similarity * 100)
        enriched["similarity_score"] = round(overall_similarity, 4)
        ranked.append(enriched)

    ranked.sort(
        key=lambda item: (
            -float(item.get("similarity_score") or 0),
            str(item.get("product_name") or ""),
        )
    )
    return ranked[:limit]
