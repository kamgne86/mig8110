from config import db, TABLE_NAME
from typing import List, Dict, Any, Optional

def execute_query(sql: str, params: List[Any] = None) -> List[Dict]:
    """Exécute une requête et retourne les résultats en dicts"""
    result = db.execute(sql, params or [])
    cols = [c[0] for c in result.description]
    rows = result.fetchall()
    return [dict(zip(cols, row)) for row in rows]

def get_products_list(q: Optional[str] = None, brand: Optional[str] = None) -> List[Dict]:
    """Liste des produits avec filtres (TOUS les résultats)"""
    sql = f"""
        SELECT code, product_name, brands,
               energy_kcal_100g, fat_100g, salt_100g, sugars_100g,
               nutriscore_grade, ecoscore_grade, front_url
        FROM {TABLE_NAME}
        WHERE 1=1
    """
    params = []

    if q:
        sql += " AND lower(product_name) LIKE ?"
        params.append(f"%{q.lower()}%")
    if brand:
        sql += " AND lower(brands) LIKE ?"
        params.append(f"%{brand.lower()}%")

    sql += " ORDER BY product_name NULLS LAST"

    return execute_query(sql, params)

def get_product_by_code(code: str) -> Optional[Dict]:
    """Détail d'un produit par code"""
    sql = f"SELECT * FROM {TABLE_NAME} WHERE code = ?"
    result = db.execute(sql, [code])
    row = result.fetchone()
    
    if not row:
        return None
    
    cols = [c[0] for c in result.description]
    return dict(zip(cols, row))
