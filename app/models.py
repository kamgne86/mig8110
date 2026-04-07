import logging
import re
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

from config import SILVER_SCHEMA, TABLE_NAME, get_conn

logger = logging.getLogger(__name__)

CATEGORIES_TABLE = f"{SILVER_SCHEMA}.categories"
PRODUCT_CATEGORIES_TABLE = f"{SILVER_SCHEMA}.product_categories"
INGREDIENTS_TABLE = f"{SILVER_SCHEMA}.ingredients"
PRODUCT_INGREDIENTS_TABLE = f"{SILVER_SCHEMA}.product_ingredients"


def clean_name(name: str) -> str:
    """Nettoie les préfixes de langue (en:, fr:, ar:, etc.) et remplace - par espaces."""
    if not name:
        return name
    cleaned = re.sub(r'^[a-z]{2,3}:', '', name)
    cleaned = cleaned.replace('-', ' ')
    return cleaned.strip().capitalize()


@contextmanager
def get_connection():
    """Context manager pour les connexions DuckDB."""
    conn = get_conn()
    try:
        yield conn
    finally:
        conn.close()


def execute_query(sql: str, params: List[Any] = None) -> List[Dict]:
    """Exécute une requête sur une connexion dédiée et la ferme aussitôt."""
    with get_connection() as conn:
        result = conn.execute(sql, params or [])
        cols = [c[0] for c in result.description]
        rows = result.fetchall()
        return [dict(zip(cols, row)) for row in rows]


def get_products_list(
    q: Optional[str] = None,
    brand: Optional[str] = None,
    ingredient: Optional[str] = None,
    category: Optional[str] = None,
    limit: int = 500,
) -> List[Dict]:
    """Liste des produits avec filtres, plafonnée à `limit` résultats."""
    sql = f"""
        WITH product_cats AS (
            SELECT pc.code, GROUP_CONCAT(DISTINCT c.category_name, '|') as categories
            FROM {PRODUCT_CATEGORIES_TABLE} pc
            LEFT JOIN {CATEGORIES_TABLE} c ON pc.category_id = c.category_id
            GROUP BY pc.code
        )
        SELECT
            p.code, p.product_name, p.brands,
            p.energy_kcal_100g, p.fat_100g, p.salt_100g, p.sugars_100g,
            p.saturated_fat_100g, p.fiber_100g, p.carbohydrates_100g, p.proteins_100g,
            p.calcium_100g, p.iron_100g, p.potassium_100g,
            p.nutriscore_grade, p.ecoscore_grade, p.front_url,
            COALESCE(pc.categories, '') as categories
        FROM {TABLE_NAME} p
        LEFT JOIN product_cats pc ON p.code = pc.code
        WHERE 1=1
    """
    params = []

    if q:
        sql += " AND lower(p.product_name) LIKE ?"
        params.append(f"%{q.lower()}%")
    if brand:
        sql += " AND lower(p.brands) LIKE ?"
        params.append(f"%{brand.lower()}%")
    if ingredient:
        ingredient_search = ingredient.lower().replace(' ', '-')
        sql += f"""
            AND p.code IN (
                SELECT pi.code
                FROM {PRODUCT_INGREDIENTS_TABLE} pi
                JOIN {INGREDIENTS_TABLE} i ON pi.ingredient_id = i.ingredient_id
                WHERE (
                    lower(i.ingredient_name) LIKE ?
                    OR lower(regexp_replace(i.ingredient_name, '^[a-z]{{2,3}}:', '')) LIKE ?
                )
            )
        """
        params.append(f"%{ingredient_search}%")
        params.append(f"%{ingredient_search}%")
    if category:
        category_search = category.lower().replace(' ', '-')
        sql += f"""
            AND p.code IN (
                SELECT pc.code
                FROM {PRODUCT_CATEGORIES_TABLE} pc
                JOIN {CATEGORIES_TABLE} c ON pc.category_id = c.category_id
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
        if product.get('categories') and product['categories'].strip():
            raw_cats = product['categories'].split('|')
            product['categories'] = [clean_name(cat) for cat in raw_cats]
        else:
            product['categories'] = []

    return results


def get_all_categories() -> List[Dict]:
    """Récupère toutes les catégories avec hiérarchie parent/enfant."""
    sql = f"""
        SELECT category_id, category_name, parent_category_id
        FROM {CATEGORIES_TABLE}
        ORDER BY category_name
    """
    return execute_query(sql)


def get_product_by_code(code: str) -> Optional[Dict]:
    """Détail d'un produit par code avec ses catégories et ingrédients."""
    with get_connection() as conn:
        result = conn.execute(f"SELECT * FROM {TABLE_NAME} WHERE code = ?", [code])
        cols = [c[0] for c in result.description]
        row = result.fetchone()
        if not row:
            return None
        product = dict(zip(cols, row))

        # Catégories avec hiérarchie — un seul JOIN au lieu de N+1 requêtes
        cat_result = conn.execute(f"""
            SELECT DISTINCT c.category_id, c.category_name, c.parent_category_id,
                   p.category_name as parent_name
            FROM {PRODUCT_CATEGORIES_TABLE} pc
            JOIN {CATEGORIES_TABLE} c ON pc.category_id = c.category_id
            LEFT JOIN {CATEGORIES_TABLE} p ON c.parent_category_id = p.category_id
            WHERE pc.code = ?
            ORDER BY c.category_name
        """, [code])

        categories_with_hierarchy = []
        for row_cat in cat_result.fetchall():
            cat_id, cat_name, parent_id, parent_name = row_cat
            cleaned_cat = clean_name(cat_name)

            if parent_id and parent_name:
                cleaned_parent = clean_name(parent_name)
                categories_with_hierarchy.append({
                    "parent": cleaned_parent,
                    "child": cleaned_cat,
                    "display": f"{cleaned_parent} › {cleaned_cat}"
                })
            else:
                categories_with_hierarchy.append({
                    "parent": None,
                    "child": cleaned_cat,
                    "display": cleaned_cat
                })

        product["categories"] = categories_with_hierarchy

        # Ingrédients
        ing_result = conn.execute(f"""
            SELECT i.ingredient_name
            FROM {PRODUCT_INGREDIENTS_TABLE} pi
            JOIN {INGREDIENTS_TABLE} i ON pi.ingredient_id = i.ingredient_id
            WHERE pi.code = ?
            ORDER BY i.ingredient_name
        """, [code])
        raw_ingredients = [r[0] for r in ing_result.fetchall()]
        product["ingredients"] = [clean_name(ing) for ing in raw_ingredients]

        return product
