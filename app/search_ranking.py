import logging
from difflib import SequenceMatcher
from functools import lru_cache
from typing import Dict, List, Optional, Sequence, Set

from models import (
    TABLE_NAME,
    clean_name,
    execute_query,
    get_products_by_codes,
)
from similarity import normalize_text, tokenize_text

logger = logging.getLogger(__name__)

FULL_TEXT_WEIGHT = 0.4
FUZZY_WEIGHT = 0.2

MAX_INITIAL_CANDIDATES = 100
MAX_REFINED_CANDIDATES = 120


def compact_text(value: object) -> str:
    return normalize_text(value).replace(" ", "")


@lru_cache(maxsize=1)
def get_light_search_documents() -> List[Dict]:
    rows = execute_query(
        f"""
        SELECT code, product_name, brands
        FROM {TABLE_NAME}
        """
    )

    documents: List[Dict] = []
    for row in rows:
        code = str(row.get("code") or "").strip()
        name = str(row.get("product_name") or "").strip()
        brands = str(row.get("brands") or "").strip()
        if not code:
            continue

        documents.append(
            {
                "code": code,
                "product_name": name,
                "brands": brands,
                "code_norm": normalize_text(code),
                "name_norm": normalize_text(name),
                "brand_norm": normalize_text(brands),
                "name_tokens": set(tokenize_text(name)),
                "brand_tokens": set(tokenize_text(brands)),
            }
        )

    logger.info("Loaded %s lightweight search documents", len(documents))
    return documents


def get_field_match_score(
    query_norm: str,
    query_tokens: Sequence[str],
    field_norm: str,
    field_tokens: Set[str],
) -> float:
    if not query_norm or not field_norm:
        return 0.0

    query_compact = query_norm.replace(" ", "")
    field_compact = field_norm.replace(" ", "")
    if query_norm == field_norm:
        return 1.0
    if query_norm in field_norm:
        return 0.95
    if query_compact and field_compact and query_compact == field_compact:
        return 0.95
    if query_compact and field_compact and query_compact in field_compact:
        return 0.9

    token_set = {token for token in query_tokens if token}
    if not token_set or not field_tokens:
        return 0.0

    coverage = len(token_set.intersection(field_tokens)) / len(token_set)
    if coverage == 1.0:
        return 0.9
    return coverage


def get_full_text_score(query_norm: str, query_tokens: Sequence[str], document: Dict) -> float:
    code_score = 1.0 if query_norm and query_norm == document.get("code_norm") else 0.0
    name_score = get_field_match_score(
        query_norm,
        query_tokens,
        str(document.get("name_norm") or ""),
        set(document.get("name_tokens") or set()),
    )
    brand_score = get_field_match_score(
        query_norm,
        query_tokens,
        str(document.get("brand_norm") or ""),
        set(document.get("brand_tokens") or set()),
    )
    blended = (0.80 * name_score) + (0.20 * brand_score)
    return max(code_score, name_score, blended)


def get_fuzzy_ratio(query_norm: str, field_norm: str) -> float:
    if not query_norm or not field_norm:
        return 0.0
    if query_norm in field_norm:
        return 1.0
    if compact_text(query_norm) and compact_text(query_norm) in compact_text(field_norm):
        return 0.95

    best_ratio = SequenceMatcher(None, query_norm, field_norm).ratio()
    query_word_count = max(len(query_norm.split()), 1)
    field_words = field_norm.split()

    for window_size in range(query_word_count, min(len(field_words), query_word_count + 1) + 1):
        for start in range(0, len(field_words) - window_size + 1):
            segment = " ".join(field_words[start : start + window_size])
            best_ratio = max(best_ratio, SequenceMatcher(None, query_norm, segment).ratio())

    for token in field_words:
        best_ratio = max(best_ratio, SequenceMatcher(None, query_norm, token).ratio())

    return best_ratio


def get_fuzzy_score(query_norm: str, document: Dict) -> float:
    name_score = get_fuzzy_ratio(query_norm, str(document.get("name_norm") or ""))
    brand_score = get_fuzzy_ratio(query_norm, str(document.get("brand_norm") or ""))
    return max(name_score, 0.85 * brand_score)


def get_quick_fuzzy_score(query_norm: str, document: Dict) -> float:
    name_score = SequenceMatcher(None, query_norm, str(document.get("name_norm") or "")).ratio()
    brand_score = SequenceMatcher(None, query_norm, str(document.get("brand_norm") or "")).ratio()
    return max(name_score, 0.85 * brand_score)


def merge_codes(*groups: Sequence[str], limit: int) -> List[str]:
    merged: List[str] = []
    seen: Set[str] = set()

    for group in groups:
        for code in group:
            clean_code = str(code or "").strip()
            if not clean_code or clean_code in seen:
                continue
            seen.add(clean_code)
            merged.append(clean_code)
            if len(merged) >= limit:
                return merged

    return merged


def get_quick_name_brand_candidates(
    q: str,
    brand: Optional[str],
    limit: int,
) -> List[str]:
    query_norm = normalize_text(q)
    brand_norm = normalize_text(brand)
    query_tokens = tokenize_text(q)
    ranked: List[Dict] = []

    for document in get_light_search_documents():
        if brand_norm and brand_norm not in str(document.get("brand_norm") or ""):
            continue

        full_text_score = max(
            1.0 if query_norm == document.get("code_norm") else 0.0,
            get_field_match_score(
                query_norm,
                query_tokens,
                str(document.get("name_norm") or ""),
                set(document.get("name_tokens") or set()),
            ),
            0.85
            * get_field_match_score(
                query_norm,
                query_tokens,
                str(document.get("brand_norm") or ""),
                set(document.get("brand_tokens") or set()),
            ),
        )
        quick_fuzzy_score = get_quick_fuzzy_score(query_norm, document)
        ranked.append(
            {
                "code": document["code"],
                "full_text_score": full_text_score,
                "quick_fuzzy_score": quick_fuzzy_score,
            }
        )

    use_fuzzy_fallback = not any(float(item.get("full_text_score") or 0.0) > 0 for item in ranked)
    for item in ranked:
        item["score"] = (
            float(item.get("quick_fuzzy_score") or 0.0)
            if use_fuzzy_fallback
            else float(item.get("full_text_score") or 0.0)
        )

    ranked.sort(
        key=lambda item: (
            -float(item.get("score") or 0.0),
            -float(item.get("full_text_score") or 0.0),
            str(item.get("code") or ""),
        )
    )
    return [str(item["code"]) for item in ranked[:limit]]


def get_product_list_categories(product: Dict) -> List[str]:
    labels: List[str] = []
    seen: Set[str] = set()

    for category in product.get("categories", []):
        if isinstance(category, dict):
            label = str(category.get("child") or category.get("display") or "").strip()
        else:
            label = str(category or "").strip()

        normalized = normalize_text(label)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        labels.append(clean_name(label))

    return labels


def build_candidate_documents(products: Sequence[Dict]) -> List[Dict]:
    documents: List[Dict] = []

    for product in products:
        code = str(product.get("code") or "").strip()
        name = str(product.get("product_name") or "").strip()
        brands = str(product.get("brands") or "").strip()

        documents.append(
            {
                "code": code,
                "product_name": name,
                "brands": brands,
                "code_norm": normalize_text(code),
                "name_norm": normalize_text(name),
                "brand_norm": normalize_text(brands),
                "name_tokens": set(tokenize_text(name)),
                "brand_tokens": set(tokenize_text(brands)),
            }
        )

    return documents


def enrich_ranked_products(
    ranked_documents: Sequence[Dict],
    product_map: Dict[str, Dict],
    limit: int,
    fuzzy_enabled: bool,
) -> List[Dict]:
    active_weight = FUZZY_WEIGHT if fuzzy_enabled else FULL_TEXT_WEIGHT

    enriched_results: List[Dict] = []
    for item in ranked_documents[:limit]:
        code = str(item.get("code") or "").strip()
        product = product_map.get(code)
        if not product:
            continue

        full_text_score = float(item.get("full_text_score") or 0.0)
        fuzzy_score = float(item.get("fuzzy_score") or 0.0)
        final_score = FUZZY_WEIGHT * fuzzy_score if fuzzy_enabled else FULL_TEXT_WEIGHT * full_text_score

        enriched = dict(product)
        enriched["categories"] = get_product_list_categories(product)
        enriched["search_score"] = round(final_score / active_weight, 4) if active_weight else 0.0
        enriched["search_score_pct"] = round(enriched["search_score"] * 100)
        enriched["search_breakdown"] = {
            "full_text": round(full_text_score, 4),
            "fuzzy": round(fuzzy_score, 4),
        }
        enriched_results.append(enriched)

    return enriched_results


def search_products(
    q: str,
    brand: Optional[str] = None,
    limit: int = 500,
) -> List[Dict]:
    query_norm = normalize_text(q)
    if not query_norm:
        return []

    brand_norm = normalize_text(brand)
    query_tokens = tokenize_text(q)
    exact_code = next(
        (
            document["code"]
            for document in get_light_search_documents()
            if query_norm == document.get("code_norm")
            and (not brand_norm or brand_norm in str(document.get("brand_norm") or ""))
        ),
        None,
    )
    if exact_code:
        products = get_products_by_codes([exact_code])
        if not products:
            return []
        exact_product = dict(products[0])
        exact_product["search_score"] = 1.0
        exact_product["search_score_pct"] = 100
        exact_product["search_breakdown"] = {
            "full_text": 1.0,
            "fuzzy": 0.0,
        }
        return [exact_product]

    initial_codes = get_quick_name_brand_candidates(
        q=q,
        brand=brand,
        limit=min(MAX_INITIAL_CANDIDATES, max(limit * 2, limit)),
    )
    candidate_codes = merge_codes(initial_codes, limit=max(MAX_REFINED_CANDIDATES, limit))
    if not candidate_codes:
        return []

    products = get_products_by_codes(candidate_codes[: max(MAX_REFINED_CANDIDATES, limit)])
    product_map = {str(product.get("code") or "").strip(): product for product in products}
    documents = build_candidate_documents(products)

    ranked_documents: List[Dict] = []
    for document in documents:
        if brand_norm and brand_norm not in str(document.get("brand_norm") or ""):
            continue

        full_text_score = get_full_text_score(query_norm, query_tokens, document)
        ranked_item = dict(document)
        ranked_item["full_text_score"] = full_text_score
        ranked_item["fuzzy_score"] = 0.0
        ranked_item["ranking_score"] = 0.0
        ranked_documents.append(ranked_item)

    use_fuzzy_fallback = not any(
        float(item.get("full_text_score") or 0.0) > 0 for item in ranked_documents
    )

    for ranked_item in ranked_documents:
        fuzzy_score = (
            get_fuzzy_score(query_norm, ranked_item)
            if use_fuzzy_fallback
            else 0.0
        )
        ranking_score = FULL_TEXT_WEIGHT * float(ranked_item.get("full_text_score") or 0.0)
        ranked_item["fuzzy_score"] = fuzzy_score
        if use_fuzzy_fallback:
            ranking_score += FUZZY_WEIGHT * fuzzy_score
        ranked_item["ranking_score"] = ranking_score

    ranked_documents.sort(
        key=lambda item: (
            -float(item.get("ranking_score") or 0.0),
            -float(item.get("full_text_score") or 0.0),
            str(item.get("product_name") or ""),
        )
    )

    return enrich_ranked_products(
        ranked_documents=ranked_documents,
        product_map=product_map,
        limit=limit,
        fuzzy_enabled=use_fuzzy_fallback,
    )
