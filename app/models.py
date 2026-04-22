import logging
import os
import re
from contextlib import contextmanager
from functools import lru_cache
from typing import Any, Dict, List, Optional

from config import SILVER_SCHEMA, TABLE_NAME, get_conn

logger = logging.getLogger(__name__)

CATEGORIES_TABLE = f"{SILVER_SCHEMA}.categories"
INGREDIENTS_TABLE = f"{SILVER_SCHEMA}.ingredients"
PRODUCT_INGREDIENTS_TABLE = f"{SILVER_SCHEMA}.product_ingredients"

PRODUCT_CATEGORIES_CANDIDATES = tuple(
    filter(
        None,
        [
            os.getenv("PRODUCT_CATEGORIES_TABLE"),
            f"{SILVER_SCHEMA}.product_categories",
            "test.product_categories",
        ],
    )
)
ANCESTOR_CATEGORIES_CANDIDATES = tuple(
    filter(
        None,
        [
            os.getenv("ANCESTOR_CATEGORIES_TABLE"),
            f"{SILVER_SCHEMA}.ancetre_categories",
        ],
    )
)
INGREDIENT_ALIAS_CANDIDATES = tuple(
    filter(
        None,
        [
            os.getenv("INGREDIENT_ALIAS_TABLE"),
            f"{SILVER_SCHEMA}.ingredient_alias",
        ],
    )
)


def clean_name(name: str) -> str:
    """Clean language prefixes and replace dashes with spaces."""
    if not name:
        return name

    cleaned = re.sub(r"^[a-z]{2,3}:", "", name)
    cleaned = cleaned.replace("-", " ")
    return cleaned.strip().capitalize()


@contextmanager
def get_connection():
    """Return a dedicated DuckDB connection."""
    conn = get_conn()
    try:
        yield conn
    finally:
        conn.close()


def execute_query(sql: str, params: Optional[List[Any]] = None) -> List[Dict]:
    """Execute a query and return rows as dictionaries."""
    with get_connection() as conn:
        result = conn.execute(sql, params or [])
        cols = [c[0] for c in result.description]
        rows = result.fetchall()
        return [dict(zip(cols, row)) for row in rows]


@lru_cache(maxsize=None)
def resolve_table_name(*candidates: str) -> Optional[str]:
    """Return the first existing fully qualified table name."""
    if not candidates:
        return None

    with get_connection() as conn:
        for candidate in candidates:
            if "." not in candidate:
                logger.warning("Ignoring invalid table candidate: %s", candidate)
                continue

            schema_name, table_name = candidate.split(".", 1)
            exists = conn.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = ? AND table_name = ?
                LIMIT 1
                """,
                [schema_name, table_name],
            ).fetchone()
            if exists:
                return candidate

    return None


def get_product_categories_table() -> Optional[str]:
    return resolve_table_name(*PRODUCT_CATEGORIES_CANDIDATES)


def get_ancestor_categories_table() -> Optional[str]:
    return resolve_table_name(*ANCESTOR_CATEGORIES_CANDIDATES)


def get_ingredient_alias_table() -> Optional[str]:
    return resolve_table_name(*INGREDIENT_ALIAS_CANDIDATES)


def get_normalized_search_sql(column_name: str) -> str:
    """Normalize prefixes and separators for tolerant LIKE filters."""
    return (
        f"lower(replace(regexp_replace(coalesce({column_name}, ''), "
        f"'^[a-z]{{2,3}}:', ''), '-', ' '))"
    )


def get_category_links_sql() -> str:
    """Build a single source of truth for product/category links."""
    select_parts = []
    product_categories_table = get_product_categories_table()

    if product_categories_table:
        select_parts.append(f"SELECT code, category_id FROM {product_categories_table}")
    else:
        logger.warning("No product/category link table found; using categorie_principale only")

    select_parts.append(
        f"""
        SELECT code, categorie_principale AS category_id
        FROM {TABLE_NAME}
        WHERE categorie_principale IS NOT NULL
        """
    )

    return "\nUNION\n".join(select_parts)


def get_products_list(
    q: Optional[str] = None,
    brand: Optional[str] = None,
    ingredient: Optional[str] = None,
    category: Optional[str] = None,
    limit: int = 500,
) -> List[Dict]:
    """List products with optional filters."""
    category_links_sql = get_category_links_sql()
    sql = f"""
        WITH category_links AS (
            {category_links_sql}
        ),
        product_cats AS (
            SELECT cl.code, GROUP_CONCAT(DISTINCT c.category_name, '|') AS categories
            FROM category_links cl
            LEFT JOIN {CATEGORIES_TABLE} c ON cl.category_id = c.category_id
            GROUP BY cl.code
        )
        SELECT
            p.code, p.product_name, p.brands,
            p.energy_kcal_100g, p.fat_100g, p.salt_100g, p.sugars_100g,
            p.saturated_fat_100g, p.fiber_100g, p.carbohydrates_100g, p.proteins_100g,
            p.calcium_100g, p.iron_100g, p.potassium_100g,
            p.nutriscore_grade, p.ecoscore_grade, p.front_url,
            COALESCE(pc.categories, '') AS categories
        FROM {TABLE_NAME} p
        LEFT JOIN product_cats pc ON p.code = pc.code
        WHERE 1=1
    """
    params: List[Any] = []

    if q:
        sql += " AND lower(p.product_name) LIKE ?"
        params.append(f"%{q.lower()}%")
    if brand:
        sql += " AND lower(p.brands) LIKE ?"
        params.append(f"%{brand.lower()}%")
    if ingredient:
        ingredient_search = ingredient.lower().strip().replace("-", " ")
        ingredient_alias_table = get_ingredient_alias_table()
        ingredient_name_sql = get_normalized_search_sql("i.ingredient_name")
        ingredient_alias_sql = get_normalized_search_sql("ia.alias_name")
        ingredient_join_sql = ""
        ingredient_alias_filter_sql = ""

        if ingredient_alias_table:
            ingredient_join_sql = (
                f"LEFT JOIN {ingredient_alias_table} ia ON pi.ingredient_id = ia.ingredient_id"
            )
            ingredient_alias_filter_sql = f" OR {ingredient_alias_sql} LIKE ?"

        sql += f"""
            AND p.code IN (
                SELECT DISTINCT pi.code
                FROM {PRODUCT_INGREDIENTS_TABLE} pi
                JOIN {INGREDIENTS_TABLE} i ON pi.ingredient_id = i.ingredient_id
                {ingredient_join_sql}
                WHERE (
                    {ingredient_name_sql} LIKE ?
                    {ingredient_alias_filter_sql}
                )
            )
        """
        params.append(f"%{ingredient_search}%")
        if ingredient_alias_table:
            params.append(f"%{ingredient_search}%")
    if category:
        category_search = category.lower().replace(" ", "-")
        sql += f"""
            AND p.code IN (
                SELECT cl.code
                FROM category_links cl
                JOIN {CATEGORIES_TABLE} c ON cl.category_id = c.category_id
                WHERE (
                    lower(c.category_name) LIKE ?
                    OR lower(regexp_replace(c.category_name, '^[a-z]{{2,3}}:', '')) LIKE ?
                )
            )
        """
        params.append(f"%{category_search}%")
        params.append(f"%{category_search}%")

    sql += " ORDER BY p.product_name NULLS LAST LIMIT ?"
    params.append(limit)

    results = execute_query(sql, params)

    for product in results:
        if product.get("categories") and product["categories"].strip():
            raw_cats = product["categories"].split("|")
            product["categories"] = [clean_name(cat) for cat in raw_cats]
        else:
            product["categories"] = []

    return results


def get_all_categories() -> List[Dict]:
    """Return all categories with their direct parent when available."""
    ancestor_categories_table = get_ancestor_categories_table()
    if ancestor_categories_table:
        sql = f"""
            SELECT c.category_id, c.category_name, a.category_id_parent AS parent_category_id
            FROM {CATEGORIES_TABLE} c
            LEFT JOIN {ancestor_categories_table} a
              ON c.category_id = a.category_id AND a.distance = 1
            ORDER BY c.category_name
        """
    else:
        sql = f"""
            SELECT category_id, category_name, NULL::BIGINT AS parent_category_id
            FROM {CATEGORIES_TABLE}
            ORDER BY category_name
        """
    return execute_query(sql)


def get_products_by_codes(codes: List[str]) -> List[Dict]:
    """Return detailed products for a list of codes."""
    ordered_codes = list(dict.fromkeys(str(code).strip() for code in codes if str(code).strip()))
    if not ordered_codes:
        return []

    placeholders = ", ".join(["?"] * len(ordered_codes))

    with get_connection() as conn:
        result = conn.execute(
            f"SELECT * FROM {TABLE_NAME} WHERE code IN ({placeholders})",
            ordered_codes,
        )
        cols = [c[0] for c in result.description]
        product_map: Dict[str, Dict] = {}

        for row in result.fetchall():
            product = dict(zip(cols, row))
            product["categories"] = []
            product["ingredients"] = []
            product_map[str(product["code"])] = product

        if not product_map:
            return []

        ancestor_categories_table = get_ancestor_categories_table()
        category_links_sql = get_category_links_sql()
        if ancestor_categories_table:
            cat_sql = f"""
                WITH category_links AS (
                    {category_links_sql}
                )
                SELECT DISTINCT cl.code, c.category_name, a.category_id_parent,
                       p.category_name AS parent_name
                FROM category_links cl
                JOIN {CATEGORIES_TABLE} c ON cl.category_id = c.category_id
                LEFT JOIN {ancestor_categories_table} a
                  ON c.category_id = a.category_id AND a.distance = 1
                LEFT JOIN {CATEGORIES_TABLE} p ON a.category_id_parent = p.category_id
                WHERE cl.code IN ({placeholders})
                ORDER BY cl.code, c.category_name
            """
        else:
            cat_sql = f"""
                WITH category_links AS (
                    {category_links_sql}
                )
                SELECT DISTINCT cl.code, c.category_name, NULL AS parent_category_id,
                       NULL AS parent_name
                FROM category_links cl
                JOIN {CATEGORIES_TABLE} c ON cl.category_id = c.category_id
                WHERE cl.code IN ({placeholders})
                ORDER BY cl.code, c.category_name
            """

        cat_result = conn.execute(cat_sql, ordered_codes)
        seen_categories = {product_code: set() for product_code in product_map}

        for product_code, cat_name, parent_id, parent_name in cat_result.fetchall():
            product_code = str(product_code)
            if product_code not in product_map:
                continue

            cleaned_cat = clean_name(cat_name)
            cleaned_parent = clean_name(parent_name) if parent_name else None
            category_key = (cleaned_parent, cleaned_cat)
            if category_key in seen_categories[product_code]:
                continue
            seen_categories[product_code].add(category_key)

            if parent_id and cleaned_parent:
                product_map[product_code]["categories"].append(
                    {
                        "parent": cleaned_parent,
                        "child": cleaned_cat,
                        "display": f"{cleaned_parent} > {cleaned_cat}",
                    }
                )
            else:
                product_map[product_code]["categories"].append(
                    {
                        "parent": None,
                        "child": cleaned_cat,
                        "display": cleaned_cat,
                    }
                )

        ing_result = conn.execute(
            f"""
            SELECT pi.code, i.ingredient_name
            FROM {PRODUCT_INGREDIENTS_TABLE} pi
            JOIN {INGREDIENTS_TABLE} i ON pi.ingredient_id = i.ingredient_id
            WHERE pi.code IN ({placeholders})
            ORDER BY pi.code, pi.ingredient_order NULLS LAST, i.ingredient_name
            """,
            ordered_codes,
        )
        seen_ingredients = {product_code: set() for product_code in product_map}

        for product_code, ingredient_name in ing_result.fetchall():
            product_code = str(product_code)
            if product_code not in product_map:
                continue

            cleaned_ingredient = clean_name(ingredient_name)
            normalized_ingredient = cleaned_ingredient.lower()
            if not normalized_ingredient or normalized_ingredient in seen_ingredients[product_code]:
                continue

            seen_ingredients[product_code].add(normalized_ingredient)
            product_map[product_code]["ingredients"].append(cleaned_ingredient)

    return [product_map[product_code] for product_code in ordered_codes if product_code in product_map]


def get_product_by_code(code: str) -> Optional[Dict]:
    """Return a product with categories and ingredients."""
    products = get_products_by_codes([code])
    return products[0] if products else None
