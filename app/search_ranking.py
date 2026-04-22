import logging
from difflib import SequenceMatcher
from functools import lru_cache
from typing import Dict, List, Optional, Sequence, Set

from models import (
    CATEGORIES_TABLE,
    INGREDIENTS_TABLE,
    PRODUCT_INGREDIENTS_TABLE,
    TABLE_NAME,
    clean_name,
    execute_query,
    get_category_links_sql,
    get_ingredient_alias_table,
    get_normalized_search_sql,
    get_products_by_codes,
)
from openai_utils import OpenAIUnavailableError, get_text_embeddings, is_openai_available
from similarity import (
    cosine_similarity_dense,
    get_alias_candidates_map,
    get_exact_ingredient_map,
    normalize_text,
    tokenize_text,
)

logger = logging.getLogger(__name__)

FULL_TEXT_WEIGHT = 0.4
ALIAS_MATCH_WEIGHT = 0.3
FUZZY_WEIGHT = 0.2
EMBEDDING_WEIGHT = 0.1

MAX_INITIAL_CANDIDATES = 100
MAX_SQL_CANDIDATES = 100
MAX_REFINED_CANDIDATES = 120
MAX_EMBEDDING_CANDIDATES = 60
MAX_ALIAS_NGRAM = 3


def dedupe_clean_values(values: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()

    for value in values:
        cleaned = clean_name(value)
        key = normalize_text(cleaned)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(cleaned)

    return out


def dedupe_normalized_values(values: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()

    for value in values:
        key = normalize_text(value)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)

    return out


def join_normalized_values(values: Sequence[str]) -> str:
    return " | ".join(dedupe_normalized_values(values))


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
    category_score = get_field_match_score(
        query_norm,
        query_tokens,
        str(document.get("categories_text") or ""),
        set(document.get("category_tokens") or set()),
    )
    ingredient_score = get_field_match_score(
        query_norm,
        query_tokens,
        str(document.get("ingredients_text") or ""),
        set(document.get("ingredient_tokens") or set()),
    )
    alias_score = get_field_match_score(
        query_norm,
        query_tokens,
        str(document.get("ingredient_aliases_text") or ""),
        set(document.get("alias_tokens") or set()),
    )

    blended = (
        (0.50 * name_score)
        + (0.15 * brand_score)
        + (0.15 * category_score)
        + (0.15 * ingredient_score)
        + (0.05 * alias_score)
    )
    return max(code_score, name_score, blended)


def iter_query_ngrams(query_norm: str) -> List[str]:
    tokens = query_norm.split()
    phrases: List[str] = []

    if query_norm:
        phrases.append(query_norm)

    max_ngram = min(MAX_ALIAS_NGRAM, len(tokens))
    for size in range(max_ngram, 0, -1):
        for start in range(0, len(tokens) - size + 1):
            phrase = " ".join(tokens[start : start + size]).strip()
            if phrase:
                phrases.append(phrase)

    return list(dict.fromkeys(phrases))


def get_query_alias_profile(query: str) -> Dict:
    query_norm = normalize_text(query)
    if not query_norm:
        return {
            "canonical": "",
            "canonical_norm": "",
            "matched_phrase": "",
            "source": "",
            "confidence": 0.0,
        }

    exact_map = get_exact_ingredient_map()
    alias_map = get_alias_candidates_map()
    best_match: Optional[Dict] = None

    for phrase in iter_query_ngrams(query_norm):
        candidate: Optional[Dict] = None
        if phrase in exact_map:
            candidate = {
                "canonical": exact_map[phrase],
                "matched_phrase": phrase,
                "source": "exact_ingredient",
                "confidence": 1.0,
            }
        elif phrase in alias_map:
            aliases = alias_map[phrase]
            candidate = {
                "canonical": aliases[0],
                "matched_phrase": phrase,
                "source": "alias_table",
                "confidence": 0.95 if len(aliases) == 1 else 0.8,
            }

        if not candidate:
            continue

        if not best_match or (
            candidate["confidence"],
            len(candidate["matched_phrase"].split()),
            len(candidate["matched_phrase"]),
        ) > (
            best_match["confidence"],
            len(best_match["matched_phrase"].split()),
            len(best_match["matched_phrase"]),
        ):
            best_match = candidate

    if not best_match:
        return {
            "canonical": "",
            "canonical_norm": "",
            "matched_phrase": "",
            "source": "",
            "confidence": 0.0,
        }

    best_match["canonical_norm"] = normalize_text(best_match["canonical"])
    return best_match


def get_alias_match_score(query_alias: Dict, document: Dict) -> float:
    canonical_norm = str(query_alias.get("canonical_norm") or "").strip()
    matched_phrase = str(query_alias.get("matched_phrase") or "").strip()
    if not canonical_norm:
        return 0.0

    ingredient_keys = set(document.get("ingredients_norm") or [])
    alias_keys = set(document.get("ingredient_aliases_norm") or [])
    if canonical_norm in ingredient_keys:
        return 1.0
    if matched_phrase and matched_phrase in alias_keys:
        return 0.95
    if canonical_norm in alias_keys:
        return 0.90

    canonical_tokens = set(tokenize_text(canonical_norm))
    if not canonical_tokens:
        return 0.0

    ingredient_overlap = len(canonical_tokens.intersection(document.get("ingredient_tokens") or set()))
    alias_overlap = len(canonical_tokens.intersection(document.get("alias_tokens") or set()))
    ingredient_score = ingredient_overlap / len(canonical_tokens)
    alias_score = alias_overlap / len(canonical_tokens)
    return max(0.80 * ingredient_score, 0.70 * alias_score)


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
    category_score = get_fuzzy_ratio(query_norm, str(document.get("categories_text") or ""))
    ingredient_score = get_fuzzy_ratio(query_norm, str(document.get("ingredients_text") or ""))
    alias_score = get_fuzzy_ratio(query_norm, str(document.get("ingredient_aliases_text") or ""))

    return max(
        name_score,
        0.85 * brand_score,
        0.80 * category_score,
        0.75 * ingredient_score,
        0.70 * alias_score,
    )


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
        initial_score = (0.75 * full_text_score) + (0.25 * quick_fuzzy_score)
        ranked.append(
            {
                "code": document["code"],
                "score": initial_score,
                "full_text_score": full_text_score,
            }
        )

    ranked.sort(
        key=lambda item: (
            -float(item.get("score") or 0.0),
            -float(item.get("full_text_score") or 0.0),
            str(item.get("code") or ""),
        )
    )
    return [str(item["code"]) for item in ranked[:limit]]


def get_category_candidate_codes(query_norm: str, limit: int) -> List[str]:
    if not query_norm:
        return []

    category_name_sql = get_normalized_search_sql("c.category_name")
    rows = execute_query(
        f"""
        WITH category_links AS (
            {get_category_links_sql()}
        )
        SELECT DISTINCT cl.code
        FROM category_links cl
        JOIN {CATEGORIES_TABLE} c ON cl.category_id = c.category_id
        WHERE {category_name_sql} LIKE ?
        LIMIT ?
        """,
        [f"%{query_norm}%", limit],
    )
    return [str(row.get("code") or "").strip() for row in rows if str(row.get("code") or "").strip()]


def get_ingredient_candidate_codes(query_norm: str, query_alias: Dict, limit: int) -> List[str]:
    terms = [query_norm]
    canonical_norm = str(query_alias.get("canonical_norm") or "").strip()
    if canonical_norm and canonical_norm not in terms:
        terms.append(canonical_norm)

    ingredient_name_sql = get_normalized_search_sql("i.ingredient_name")
    ingredient_alias_table = get_ingredient_alias_table()
    ingredient_alias_sql = get_normalized_search_sql("ia.alias_name")
    join_sql = ""
    term_clauses: List[str] = []
    params: List[object] = []

    if ingredient_alias_table:
        join_sql = f"LEFT JOIN {ingredient_alias_table} ia ON pi.ingredient_id = ia.ingredient_id"

    for term in terms:
        if not term:
            continue
        clause_parts = [f"{ingredient_name_sql} LIKE ?"]
        params.append(f"%{term}%")
        if ingredient_alias_table:
            clause_parts.append(f"{ingredient_alias_sql} LIKE ?")
            params.append(f"%{term}%")
        term_clauses.append("(" + " OR ".join(clause_parts) + ")")

    if not term_clauses:
        return []

    rows = execute_query(
        f"""
        SELECT DISTINCT pi.code
        FROM {PRODUCT_INGREDIENTS_TABLE} pi
        JOIN {INGREDIENTS_TABLE} i ON pi.ingredient_id = i.ingredient_id
        {join_sql}
        WHERE {" OR ".join(term_clauses)}
        LIMIT ?
        """,
        [*params, limit],
    )
    return [str(row.get("code") or "").strip() for row in rows if str(row.get("code") or "").strip()]


def get_candidate_aliases(codes: Sequence[str]) -> Dict[str, List[str]]:
    ingredient_alias_table = get_ingredient_alias_table()
    ordered_codes = list(dict.fromkeys(str(code).strip() for code in codes if str(code).strip()))
    if not ordered_codes or not ingredient_alias_table:
        return {}

    placeholders = ", ".join(["?"] * len(ordered_codes))
    rows = execute_query(
        f"""
        SELECT pi.code, ia.alias_name
        FROM {PRODUCT_INGREDIENTS_TABLE} pi
        JOIN {ingredient_alias_table} ia ON pi.ingredient_id = ia.ingredient_id
        WHERE pi.code IN ({placeholders})
          AND ia.alias_name IS NOT NULL
          AND trim(ia.alias_name) <> ''
        """,
        ordered_codes,
    )

    alias_map: Dict[str, List[str]] = {}
    for row in rows:
        code = str(row.get("code") or "").strip()
        alias_name = str(row.get("alias_name") or "").strip()
        if code and alias_name:
            alias_map.setdefault(code, []).append(alias_name)

    return alias_map


def get_product_category_labels(product: Dict) -> List[str]:
    labels: List[str] = []
    for category in product.get("categories", []):
        if isinstance(category, dict):
            parent = str(category.get("parent") or "").strip()
            child = str(category.get("child") or "").strip()
            display = str(category.get("display") or "").strip()
            if child:
                labels.append(child)
            if parent:
                labels.append(parent)
            if display:
                labels.append(display)
        elif isinstance(category, str):
            labels.append(category)
    return dedupe_clean_values(labels)


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


def build_candidate_documents(products: Sequence[Dict], alias_map: Dict[str, List[str]]) -> List[Dict]:
    documents: List[Dict] = []

    for product in products:
        code = str(product.get("code") or "").strip()
        categories = get_product_category_labels(product)
        ingredients = dedupe_clean_values(product.get("ingredients", []))
        ingredient_aliases = dedupe_clean_values(alias_map.get(code, []))
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
                "categories_norm": dedupe_normalized_values(categories),
                "ingredients_norm": dedupe_normalized_values(ingredients),
                "ingredient_aliases_norm": dedupe_normalized_values(ingredient_aliases),
                "name_tokens": set(tokenize_text(name)),
                "brand_tokens": set(tokenize_text(brands)),
                "category_tokens": set(token for value in categories for token in tokenize_text(value)),
                "ingredient_tokens": set(token for value in ingredients for token in tokenize_text(value)),
                "alias_tokens": set(token for value in ingredient_aliases for token in tokenize_text(value)),
                "categories_text": join_normalized_values(categories),
                "ingredients_text": join_normalized_values(ingredients),
                "ingredient_aliases_text": join_normalized_values(ingredient_aliases),
                "embedding_text": " | ".join(
                    part
                    for part in [
                        name,
                        brands,
                        *categories[:4],
                        *ingredients[:8],
                    ]
                    if str(part).strip()
                ),
            }
        )

    return documents


def apply_embedding_scores(query: str, ranked_documents: List[Dict]) -> bool:
    if not ranked_documents or not is_openai_available():
        return False

    try:
        texts = [query] + [str(item.get("embedding_text") or "") for item in ranked_documents]
        embeddings = get_text_embeddings(texts)
    except OpenAIUnavailableError as exc:
        logger.warning("Search embedding disabled for this run: %s", exc)
        return False

    query_vector = embeddings[0]
    for index, item in enumerate(ranked_documents, start=1):
        item["embedding_score"] = max(0.0, cosine_similarity_dense(query_vector, embeddings[index]))
    return True


def enrich_ranked_products(
    ranked_documents: Sequence[Dict],
    product_map: Dict[str, Dict],
    limit: int,
    embedding_enabled: bool,
) -> List[Dict]:
    active_weight = FULL_TEXT_WEIGHT + ALIAS_MATCH_WEIGHT + FUZZY_WEIGHT
    if embedding_enabled:
        active_weight += EMBEDDING_WEIGHT

    enriched_results: List[Dict] = []
    for item in ranked_documents[:limit]:
        code = str(item.get("code") or "").strip()
        product = product_map.get(code)
        if not product:
            continue

        full_text_score = float(item.get("full_text_score") or 0.0)
        alias_match_score = float(item.get("alias_match_score") or 0.0)
        fuzzy_score = float(item.get("fuzzy_score") or 0.0)
        embedding_score = float(item.get("embedding_score") or 0.0)

        final_score = (
            (FULL_TEXT_WEIGHT * full_text_score)
            + (ALIAS_MATCH_WEIGHT * alias_match_score)
            + (FUZZY_WEIGHT * fuzzy_score)
        )
        if embedding_enabled:
            final_score += EMBEDDING_WEIGHT * embedding_score

        enriched = dict(product)
        enriched["categories"] = get_product_list_categories(product)
        enriched["search_score"] = round(final_score / active_weight, 4) if active_weight else 0.0
        enriched["search_score_pct"] = round(enriched["search_score"] * 100)
        enriched["search_breakdown"] = {
            "full_text": round(full_text_score, 4),
            "alias_match": round(alias_match_score, 4),
            "fuzzy": round(fuzzy_score, 4),
            "embedding": round(embedding_score, 4) if embedding_enabled else 0.0,
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
    query_alias = get_query_alias_profile(q)
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
            "alias_match": 0.0,
            "fuzzy": 1.0,
            "embedding": 0.0,
        }
        return [exact_product]

    initial_codes = get_quick_name_brand_candidates(
        q=q,
        brand=brand,
        limit=min(MAX_INITIAL_CANDIDATES, max(limit * 2, limit)),
    )
    category_codes = get_category_candidate_codes(query_norm, MAX_SQL_CANDIDATES)
    ingredient_codes = get_ingredient_candidate_codes(query_norm, query_alias, MAX_SQL_CANDIDATES)
    candidate_codes = merge_codes(
        initial_codes,
        category_codes,
        ingredient_codes,
        limit=max(MAX_REFINED_CANDIDATES, limit),
    )
    if not candidate_codes:
        return []

    products = get_products_by_codes(candidate_codes[: max(MAX_REFINED_CANDIDATES, limit)])
    product_map = {str(product.get("code") or "").strip(): product for product in products}
    alias_map = get_candidate_aliases(product_map.keys())
    documents = build_candidate_documents(products, alias_map)

    ranked_documents: List[Dict] = []
    for document in documents:
        if brand_norm and brand_norm not in str(document.get("brand_norm") or ""):
            continue

        full_text_score = get_full_text_score(query_norm, query_tokens, document)
        alias_match_score = get_alias_match_score(query_alias, document)
        fuzzy_score = get_fuzzy_score(query_norm, document)
        non_embedding_score = (
            (FULL_TEXT_WEIGHT * full_text_score)
            + (ALIAS_MATCH_WEIGHT * alias_match_score)
            + (FUZZY_WEIGHT * fuzzy_score)
        )

        ranked_item = dict(document)
        ranked_item["full_text_score"] = full_text_score
        ranked_item["alias_match_score"] = alias_match_score
        ranked_item["fuzzy_score"] = fuzzy_score
        ranked_item["embedding_score"] = 0.0
        ranked_item["non_embedding_score"] = non_embedding_score
        ranked_documents.append(ranked_item)

    ranked_documents.sort(
        key=lambda item: (
            -float(item.get("non_embedding_score") or 0.0),
            -float(item.get("full_text_score") or 0.0),
            str(item.get("product_name") or ""),
        )
    )

    embedding_candidates = ranked_documents[: min(MAX_EMBEDDING_CANDIDATES, max(limit, 1))]
    embedding_enabled = apply_embedding_scores(q, embedding_candidates)
    if embedding_enabled:
        ranked_documents.sort(
            key=lambda item: (
                -(
                    float(item.get("non_embedding_score") or 0.0)
                    + (EMBEDDING_WEIGHT * float(item.get("embedding_score") or 0.0))
                ),
                -float(item.get("full_text_score") or 0.0),
                str(item.get("product_name") or ""),
            )
        )

    return enrich_ranked_products(
        ranked_documents=ranked_documents,
        product_map=product_map,
        limit=limit,
        embedding_enabled=embedding_enabled,
    )
