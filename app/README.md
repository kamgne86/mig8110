Ce dossier contient le code source de l'application principale, incluant l'API et le GUI.

# OFF Canada API - Documentation

## Lancement local (développement)

### Prérequis

```bash
# Installer les dépendances
pip install -r requirements.txt

# Créer .env avec tes tokens MotherDuck
echo "DUCKDB_TOKEN=ton_token" >> .env
echo "DUCKDB_DB=ton_database" >> .env
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
GET /products?ingredients=tomate        # Produits contenant l'ingrédient

# Produit spécifique
GET /products/0068200466583             # Détails complets
```

### URLs disponibles :

```bash
Swagger docs : http://localhost:8001/docs
Health check : http://localhost:8001/health
Frontend     : http://localhost:8001/static/index.html
Recherche    : http://localhost:8001/products?q=milk&brand=kroger
```
