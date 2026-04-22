Ce dossier contient le code source de l'application principale, incluant l'API et le GUI.

# OFF Canada API - Documentation

## Lancement local (developpement)

### Prerequis

```bash
# Installer les dependances
pip install -r requirements.txt

# Creer .env avec tes tokens MotherDuck
echo "DUCKDB_TOKEN=ton_token" >> .env
echo "DUCKDB_DB=ton_database" >> .env

# Optionnel : activer la normalisation LLM et les embeddings OpenAI
echo "OPENAI_API_KEY=ton_openai_api_key" >> .env
echo "OPENAI_ALIAS_MODEL=gpt-4o-mini" >> .env
echo "OPENAI_EMBEDDING_MODEL=text-embedding-3-small" >> .env

# Optionnel : changer le fichier de cache persistant des alias
echo "ALIAS_CACHE_PATH=./alias_normalization_cache.json" >> .env
```

### Lancer l'API

```bash
uvicorn main:app --reload --port 8001
```

## Docker (production)

### Build image

```bash
docker build -t off-api .
```

### Lancer conteneur

```bash
docker run -d -p 8001:8001 --env-file .env off-api
```

## Utilisation

```bash
# Recherche produits
GET /products?q=milk                    # Tous les laits
GET /products?q=milk&brand=kroger       # Laits Kroger seulement
GET /products?ingredient=tomate         # Produits contenant l'ingredient

# Produit specifique
GET /products/0068200466583             # Details complets

# Produits similaires rerankes
GET /products/0068200466583/similar
```

### URLs disponibles

```bash
Swagger docs : http://localhost:8001/docs
Health check : http://localhost:8001/health
Frontend     : http://localhost:8001/static/index.html
Recherche    : http://localhost:8001/products?q=milk&brand=kroger
```
