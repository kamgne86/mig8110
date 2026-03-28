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
