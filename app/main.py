from config import get_conn
from fastapi import FastAPI
from routers import products
from fastapi.staticfiles import StaticFiles

app = FastAPI(
    title="OFF Canada API",
    description="API pour explorer les produits alimentaires canadiens",
    version="1.0.0"
)

app.mount("/static", StaticFiles(directory="static"), name="static")

app.include_router(products.router)

@app.get("/health", tags=["health"])
def health():
    conn = get_conn()
    try:
        conn.execute("SELECT 1")
    finally:
        conn.close()
    return {"status": "ok"}
