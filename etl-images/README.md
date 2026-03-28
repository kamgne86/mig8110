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

### validate_data

Valide les enregistrements du parquet filtré. Les enregistrements valides et invalides sont uploadés séparément sur S3.

```bash
python run.py \
  --command=validate_data \
  --input_file_key=raw/openfoodfacts_filtered.parquet \
  --output_file_key=staging/openfoodfacts_valid.parquet \
  --invalid_file_key=staging/openfoodfacts_invalid.parquet
```

### filter_data

Sélectionne uniquement les colonnes pertinentes depuis un parquet brut et uploade le résultat sur S3. Permet de réduire l'empreinte mémoire des étapes suivantes du pipeline.

```bash
python run.py \
  --command=filter_data \
  --input_file_key=raw/openfoodfacts.parquet \
  --output_file_key=raw/openfoodfacts_filtered.parquet \
  --columns=code,brands,product_name,product_quantity,product_quantity_unit,quantity,serving_quantity,serving_size,categories_tags,countries_tags,ecoscore_score,ecoscore_grade,images,ingredients_tags,ingredients,nutriscore_score,nutriscore_grade,nutriments
```
