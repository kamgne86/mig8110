from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from models import get_all_categories, get_product_by_code, get_products_list
from schemas import ProductListItem

router = APIRouter(prefix="/products", tags=["products"])


@router.get("/", response_model=list[ProductListItem])
def list_products(
    q: Optional[str] = Query(None, description="Recherche par nom de produit"),
    brand: Optional[str] = Query(None, description="Filtre par marque"),
    ingredient: Optional[str] = Query(None, description="Recherche par ingrédient"),
    category: Optional[str] = Query(None, description="Filtrer par catégorie"),
    limit: int = Query(500, ge=1, le=1000, description="Nombre maximum de résultats"),
):
    return get_products_list(q, brand, ingredient, category, limit)


@router.get("/categories", tags=["categories"])
def list_categories():
    """Retourne toutes les catégories avec hiérarchie."""
    return get_all_categories()


@router.get("/{code}")
def get_product(code: str):
    product = get_product_by_code(code)
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    return product
