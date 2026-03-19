from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from models import get_products_list, get_product_by_code

router = APIRouter(prefix="/products", tags=["products"])

@router.get("/")
def list_products(
    q: Optional[str] = Query(None, description="Recherche par nom de produit"),
    brand: Optional[str] = Query(None, description="Filtre par marque"),
):
    return get_products_list(q, brand)

@router.get("/{code}")
def get_product(code: str):
    product = get_product_by_code(code)
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    return product
