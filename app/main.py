from config import db
from fastapi import FastAPI
from routers import products

app = FastAPI(
    title="OFF Canada API",
    description="API pour explorer les produits alimentaires canadiens",
    version="1.0.0"
)

app.include_router(products.router)

@app.get("/health", tags=["health"])
def health():
    db.execute("SELECT 1")
    return {"status": "ok"}
