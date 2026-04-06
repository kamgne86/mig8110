from pydantic import BaseModel
from typing import Optional


class CategorySchema(BaseModel):
    parent: Optional[str] = None
    child: str
    display: str


class ProductListItem(BaseModel):
    code: str
    product_name: Optional[str] = None
    brands: Optional[str] = None
    energy_kcal_100g: Optional[float] = None
    fat_100g: Optional[float] = None
    salt_100g: Optional[float] = None
    sugars_100g: Optional[float] = None
    saturated_fat_100g: Optional[float] = None
    fiber_100g: Optional[float] = None
    carbohydrates_100g: Optional[float] = None
    proteins_100g: Optional[float] = None
    calcium_100g: Optional[float] = None
    iron_100g: Optional[float] = None
    potassium_100g: Optional[float] = None
    nutriscore_grade: Optional[str] = None
    ecoscore_grade: Optional[str] = None
    front_url: Optional[str] = None
    categories: list[str] = []

    model_config = {"from_attributes": True}


class ProductDetail(BaseModel):
    code: str
    product_name: Optional[str] = None
    brands: Optional[str] = None
    energy_kcal_100g: Optional[float] = None
    fat_100g: Optional[float] = None
    salt_100g: Optional[float] = None
    sugars_100g: Optional[float] = None
    saturated_fat_100g: Optional[float] = None
    fiber_100g: Optional[float] = None
    carbohydrates_100g: Optional[float] = None
    proteins_100g: Optional[float] = None
    calcium_100g: Optional[float] = None
    iron_100g: Optional[float] = None
    potassium_100g: Optional[float] = None
    nutriscore_grade: Optional[str] = None
    ecoscore_grade: Optional[str] = None
    front_url: Optional[str] = None
    categories: list[CategorySchema] = []
    ingredients: list[str] = []
    ingredients_n: Optional[float] = None
    quantity: Optional[str] = None
    serving_size: Optional[str] = None
    trans_fat_100g: Optional[float] = None
    cholesterol_100g: Optional[float] = None
    sodium_100g: Optional[float] = None

    model_config = {"from_attributes": True, "extra": "allow"}
