# ETL Images

Ce dossier contient le code source des images Docker utilisées pour exécuter les tâches Airflow. Ces images sont orchestrées et lancées via `DockerOperator` ou `KubernetesPodOperator`, permettant une exécution isolée et reproductible des processus ETL.

## Commandes

Les commandes s'exécutent depuis le dossier `etl-images/` avec les variables d'environnement S3 définies :

```bash
export S3_BUCKET=<bucket>
export S3_ENDPOINT=<endpoint>
export S3_ACCESS_KEY=<access_key>
export S3_SECRET_KEY=<secret_key>
```

### extract_data

Télécharge le fichier parquet Open Food Facts depuis une URL (archive ZIP) et l'uploade sur S3.

```bash
python run.py \
  --command=extract_data \
  --url=https://raw.githubusercontent.com/adilblanco/mig8110/main/data/canada_products.parquet.zip \
  --output_file_key=raw/openfoodfacts.parquet
```

### filter_data

Sélectionne uniquement les colonnes pertinentes depuis un parquet brut et uploade le résultat sur S3. Permet de réduire l'empreinte mémoire des étapes suivantes du pipeline. Supporte un filtrage optionnel par pays (`--country`, substring match sur `countries_tags`) et par langue (`--lang`, match exact sur la colonne `lang`).

```bash
python run.py \
  --command=filter_data \
  --input_file_key=raw/openfoodfacts.parquet \
  --output_file_key=raw/openfoodfacts_filtered.parquet \
  --columns=code,brands,product_name,product_quantity,product_quantity_unit,quantity,serving_quantity,serving_size,categories_tags,countries_tags,lang,ecoscore_score,ecoscore_grade,images,ingredients_tags,ingredients,nutriscore_score,nutriscore_grade,nutriments \
  --country=canada \
  --lang=fr
```

### validate_data

Valide les enregistrements du parquet filtré. Les enregistrements valides et invalides sont uploadés séparément sur S3. Les métriques sont enregistrées dans la table de monitoring spécifiée.

```bash
python run.py \
  --command=validate_data \
  --input_file_key=raw/openfoodfacts_filtered.parquet \
  --output_file_key=staging/openfoodfacts_valid.parquet \
  --invalid_file_key=staging/openfoodfacts_invalid.parquet \
  --schema_name=monitoring \
  --table_name=pipeline_runs
```

### transform_data

Transforme les données validées : extrait le nom du produit, construit les URLs d'images, extrait les nutriments et projette sur le schéma final.

```bash
python run.py \
  --command=transform_data \
  --input_file_key=staging/openfoodfacts_valid.parquet \
  --output_file_key=staging/openfoodfacts_transformed.parquet
```

### load_data

Charge le parquet transformé depuis S3 et l'insère dans une table MotherDuck (DuckDB).

```bash
python run.py \
  --command=load_data \
  --input_file_key=staging/openfoodfacts_transformed.parquet \
  --table_name=canada_products \
  --schema_name=staging
```

---

## Commandes delta (chargement incrémental)

Variables d'environnement supplémentaires requises pour les commandes delta :

```bash
export DUCKDB_TOKEN=<token>
export DUCKDB_DB=<database>
```

### fetch_delta_index

Lit le fichier `index.txt` depuis Open Food Facts, trie la liste de fichiers delta chronologiquement et l'écrit dans XCom (ou dans les logs en mode local).

```bash
python run.py \
  --command=fetch_delta_index \
  --url=https://static.openfoodfacts.org/data/delta/index.txt
```

### extract_delta

Télécharge un seul fichier delta `.json.gz` en chunks de 50 MB, filtre les enregistrements par pays, sérialise les colonnes complexes (listes, dicts) en JSON strings et uploade le résultat en parquet sur S3.

```bash
python run.py \
  --command=extract_delta \
  --filename=openfoodfacts_products_1770673073_1772050745.json.gz \
  --base_url=https://static.openfoodfacts.org/data/delta/ \
  --output_file_key=off_weekly_delta_load/delta/openfoodfacts_products_1770673073_1772050745.parquet
```

### filter_delta

Sélectionne uniquement les colonnes pertinentes depuis le parquet delta. Supporte la syntaxe pipe (`target|fallback`) pour gérer les colonnes renommées entre versions de fichiers delta. Les colonnes absentes sont incluses avec des valeurs `None` pour garantir un schéma uniforme. Supporte un filtrage optionnel par langue (`--lang`, match exact sur la colonne `lang`).

> **Note :** La colonne `nutrition` est extraite à la place de `nutriments` (qui existe dans les deltas mais est majoritairement vide). Le renommage vers `nutriments` est effectué dans `transform_delta`.

```bash
python run.py \
  --command=filter_delta \
  --input_file_key=off_weekly_delta_load/delta/openfoodfacts_products_1770673073_1772050745.parquet \
  --output_file_key=off_weekly_delta_load/delta/openfoodfacts_products_1770673073_1772050745_filtered.parquet \
  --columns=code,brands,product_name,product_quantity,product_quantity_unit,quantity,serving_quantity,serving_size,categories_tags,countries_tags,lang,ecoscore_score|environmental_score_score,ecoscore_grade|environmental_score_grade,images,ingredients_tags,nutriscore_score,nutriscore_grade,nutrition \
  --lang=fr
```

### validate_delta

Valide les enregistrements du parquet filtré delta. Utilise des règles de validation spécifiques aux deltas (`validation_rules_delta.py`), notamment la vérification de la colonne `nutrition` (au lieu de `nutriments` pour le chargement initial). Les métriques sont enregistrées dans la table de monitoring spécifiée.

```bash
python run.py \
  --command=validate_delta \
  --input_file_key=off_weekly_delta_load/delta/openfoodfacts_products_1770673073_1772050745_filtered.parquet \
  --output_file_key=off_weekly_delta_load/delta/openfoodfacts_products_1770673073_1772050745_valid.parquet \
  --invalid_file_key=off_weekly_delta_load/delta/openfoodfacts_products_1770673073_1772050745_invalid.parquet \
  --schema_name=monitoring \
  --table_name=pipeline_runs
```

### transform_delta

Transforme les données validées au format delta : construit les URLs d'images depuis `images.selected`, extrait les nutriments depuis le dict plat et projette sur le schéma Silver.

```bash
python run.py \
  --command=transform_delta \
  --input_file_key=off_weekly_delta_load/delta/openfoodfacts_products_1770673073_1772050745_valid.parquet \
  --output_file_key=off_weekly_delta_load/delta/openfoodfacts_products_1770673073_1772050745_transformed.parquet
```

### load_delta

Charge le parquet transformé depuis S3 et effectue un upsert dans MotherDuck : supprime les lignes existantes dont le `code` est présent dans le fichier, puis insère toutes les lignes. Garantit qu'un produit modifié remplace son ancienne version sans doublons.

```bash
python run.py \
  --command=load_delta \
  --input_file_key=off_weekly_delta_load/delta/openfoodfacts_products_1770673073_1772050745_transformed.parquet \
  --table_name=source_transformed \
  --schema_name=staging
```

### normalize_categories

Normalise la colonne `categories_tags` à partir de la taxonomy officielle Open Food Facts. Produit deux parquets sur S3 : `categories` (référentiel OFF) et `product_categories` (table de jonction Many-to-Many). La table `products` finale est produite par `finalize_products` après l'exécution parallèle avec `normalize_ingredients`.

```bash
python run.py \
  --command=normalize_categories \
  --input_file_key=off_weekly_delta_load/delta/openfoodfacts_products_1770673073_1772050745_transformed.parquet \
  --categories_output_key=off_weekly_delta_load/delta/openfoodfacts_products_1770673073_1772050745_categories.parquet \
  --product_categories_output_key=off_weekly_delta_load/delta/openfoodfacts_products_1770673073_1772050745_product_categories.parquet
```

### normalize_ingredients

Normalise la colonne `ingredients_tags` à partir de la taxonomy officielle Open Food Facts. Produit deux parquets sur S3 : `ingredients` (référentiel OFF) et `product_ingredients` (table de jonction Many-to-Many). La table `products` finale est produite par `finalize_products` après l'exécution parallèle avec `normalize_categories`.

```bash
python run.py \
  --command=normalize_ingredients \
  --input_file_key=off_weekly_delta_load/delta/openfoodfacts_products_1770673073_1772050745_transformed.parquet \
  --ingredients_output_key=off_weekly_delta_load/delta/openfoodfacts_products_1770673073_1772050745_ingredients.parquet \
  --product_ingredients_output_key=off_weekly_delta_load/delta/openfoodfacts_products_1770673073_1772050745_product_ingredients.parquet
```

### finalize_products

Supprime `categories_tags` et `ingredients_tags` du parquet transformé pour produire la table `products` finale. Doit être exécuté après `normalize_categories` et `normalize_ingredients`.

> **Note :** Pour tester localement, enchaîner les étapes suivantes dans l'ordre.

```bash
# Étape 1 — Extraction
python run.py \
  --command=extract_delta \
  --filename=openfoodfacts_products_1775031464_1775116054.json.gz \
  --base_url=https://static.openfoodfacts.org/data/delta/ \
  --output_file_key=test/bronze/extract_delta_test.parquet \
  --country=canada \
  --columns="code,brands,product_name,product_quantity,product_quantity_unit,quantity,serving_quantity,serving_size,categories_tags,countries_tags,ecoscore_score|environmental_score_score,ecoscore_grade|environmental_score_grade,images,ingredients_tags,ingredients_n,nutriscore_score,nutriscore_grade,nutrition"

# Étape 2 — Transformation (bronze → silver)
python run.py \
  --command=transform_delta \
  --input_file_key=test/bronze/extract_delta_test.parquet \
  --output_file_key=test/silver/transform_delta_test.parquet

# Étape 3a — Normalisation des catégories (parallèle avec 3b)
python run.py \
  --command=normalize_categories \
  --input_file_key=test/silver/transform_delta_test.parquet \
  --categories_output_key=test/silver/categories_test.parquet \
  --product_categories_output_key=test/silver/product_categories_test.parquet

# Étape 3b — Normalisation des ingrédients (parallèle avec 3a)
python run.py \
  --command=normalize_ingredients \
  --input_file_key=test/silver/transform_delta_test.parquet \
  --ingredients_output_key=test/silver/ingredients_test.parquet \
  --product_ingredients_output_key=test/silver/product_ingredients_test.parquet

# Étape 4 — Finalisation de la table products
python run.py \
  --command=finalize_products \
  --input_file_key=test/silver/transform_delta_test.parquet \
  --output_file_key=test/silver/products_test.parquet
```
