from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from models import get_all_categories, get_product_by_code, get_products_list
from search_ranking import search_products
from schemas import ProductListItem, SimilarProductItem
from similarity import get_similar_products

router = APIRouter(prefix="/products", tags=["products"])


@router.get("/", response_model=list[ProductListItem])
def list_products(
    q: Optional[str] = Query(None, description="Recherche par nom de produit"),
    brand: Optional[str] = Query(None, description="Filtre par marque"),
    ingredient: Optional[str] = Query(None, description="Recherche par ingredient"),
    category: Optional[str] = Query(None, description="Filtrer par categorie"),
    limit: int = Query(500, ge=1, le=1000, description="Nombre maximum de resultats"),
):
    if q and not ingredient and not category:
        return search_products(q=q, brand=brand, limit=limit)
    return get_products_list(q, brand, ingredient, category, limit)


@router.get("/categories", tags=["categories"])
def list_categories():
    """Retourne toutes les categories avec hierarchie."""
    return get_all_categories()


@router.get("/{code}/similar", response_model=list[SimilarProductItem])
def list_similar_products(
    code: str,
    limit: int = Query(4, ge=1, le=12, description="Nombre maximum de produits similaires"),
    candidate_pool: int = Query(
        20,
        ge=8,
        le=120,
        description="Taille du pool de candidats avant classement final",
    ),
):
    items = get_similar_products(code, limit=limit, candidate_pool=candidate_pool)
    if items is None:
        raise HTTPException(status_code=404, detail="Product not found")
    return items


@router.get("/{code}")
def get_product(code: str):
    product = get_product_by_code(code)
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    return product
