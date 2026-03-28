"""
DAG : off_initial_load
======================
Chargement initial complet des produits alimentaires canadiens depuis Open Food Facts.
Ce DAG est déclenché manuellement (schedule_interval=None) et doit être exécuté une seule
fois pour initialiser les données, ou relancé pour un rechargement complet.

Pipeline :
    extract_data   : Télécharge le snapshot parquet depuis GitHub et le dépose sur S3 (Bronze)
    filter_data    : Sélectionne les colonnes utiles pour réduire l'empreinte mémoire
    load_bronze    : Charge le parquet filtré dans MotherDuck (off.raw)
    validate_data  : Sépare les enregistrements valides des invalides selon les règles définies
    transform_data : Transforme les données : product_name, URLs d'images, nutriments à plat
    load_silver    : Charge les données transformées dans MotherDuck (off.staging)
    load_rejected  : Charge les enregistrements invalides dans MotherDuck (off.staging)

Outputs S3 (bucket: bi-dev, préfixe: off_initial_load/) :
    data.parquet              : Snapshot brut Open Food Facts
    data_filtered.parquet     : Snapshot filtré (colonnes sélectionnées)
    data_valid.parquet        : Enregistrements valides
    data_invalid.parquet      : Enregistrements invalides (quarantaine)
    data_transformed.parquet  : Enregistrements transformés prêts pour le chargement

Outputs MotherDuck (base: off) :
    raw.canada_products        : Données brutes filtrées (Bronze)
    staging.source_transformed : Table cible principale — données transformées (Silver),
                                 source des produits alimentaires pour l'application
    staging.source_rejected    : Données invalides pour inspection
    monitoring.pipeline_runs   : Métriques d'exécution (records_in, records_out, rejection_rate)
"""
import datetime
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from plugins.operators.duckdb_operator import DuckDBOperator
from plugins.operators.custom_kubernetes_operator import CustomKubernetesPodOperator


# Image
IMAGE  = "mig8110/etl-images:1.0.0"
DAG_ID = "off_initial_load"

args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2026, 1, 1),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=60)
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=args,
    schedule_interval=None,
    catchup=False,
    tags=['mig8110', 'off']
)

# Connexions
s3_env_vars = {
    "S3_ENDPOINT":   "{{ conn.s3_conn.host }}",
    "S3_ACCESS_KEY": "{{ conn.s3_conn.login }}",
    "S3_SECRET_KEY": "{{ conn.s3_conn.password }}",
    "S3_BUCKET":     "{{ conn.s3_conn.schema }}",
}

duckdb_env_vars = {
    "DUCKDB_TOKEN": "{{ conn.duckdb_default.password }}",
    "DUCKDB_DB":    "{{ conn.duckdb_default.schema }}",
}

airflow_env_vars = {
    "AIRFLOW_CTX_DAG_RUN_ID": "{{ run_id }}",
}

# Base de données
DATABASE_NAME  = "off"
RAW_SCHEMA     = "raw"
STAGING_SCHEMA = "staging"

RAW_TABLE_NAME      = "canada_products"
STAGING_TABLE_NAME  = "source_transformed"
REJECTED_TABLE_NAME = "source_rejected"

# Clés S3 préfixées par dag_id pour isoler les fichiers dans le bucket
RAW_FILE_KEY         = f"{DAG_ID}/data.parquet"
FILTERED_FILE_KEY    = f"{DAG_ID}/data_filtered.parquet"
VALID_FILE_KEY       = f"{DAG_ID}/data_valid.parquet"
INVALID_FILE_KEY     = f"{DAG_ID}/data_invalid.parquet"
TRANSFORMED_FILE_KEY = f"{DAG_ID}/data_transformed.parquet"

# Colonnes à conserver lors du filtrage
FILTER_COLUMNS = ",".join([
    "code", "brands", "product_name", "product_quantity", "product_quantity_unit",
    "quantity", "serving_quantity", "serving_size", "categories_tags", "countries_tags",
    "ecoscore_score", "ecoscore_grade", "images", "ingredients_tags", "ingredients",
    "nutriscore_score", "nutriscore_grade", "nutriments",
])

with dag:

    start = EmptyOperator(task_id='start')

    create_schemas = DuckDBOperator(
        dag=dag,
        task_id='create-schemas',
        sql=f"""
            CREATE SCHEMA IF NOT EXISTS {DATABASE_NAME}.{RAW_SCHEMA};
            CREATE SCHEMA IF NOT EXISTS {DATABASE_NAME}.{STAGING_SCHEMA};
        """,
        duckdb_conn_id='duckdb_default'
    )

    # Télécharge le snapshot initial des produits canadiens depuis GitHub
    # et le dépose sur S3 au format parquet brut (couche Bronze)
    extract_data = CustomKubernetesPodOperator(
        dag=dag,
        name='extract-data',
        image=IMAGE,
        env_vars={**s3_env_vars},
        arguments=[
            "--command", "extract_data",
            "--output_file_key", RAW_FILE_KEY,
            "--url", "https://raw.githubusercontent.com/adilblanco/mig8110/main/data/canada_products.parquet.zip"
        ]
    )

    # Sélectionne uniquement les colonnes nécessaires au pipeline
    # afin de réduire l'empreinte mémoire des étapes suivantes
    filter_data = CustomKubernetesPodOperator(
        dag=dag,
        name='filter-data',
        image=IMAGE,
        env_vars={**s3_env_vars},
        arguments=[
            "--command", "filter_data",
            "--input_file_key",  RAW_FILE_KEY,
            "--output_file_key", FILTERED_FILE_KEY,
            "--columns", FILTER_COLUMNS,
        ]
    )

    load_bronze = CustomKubernetesPodOperator(
        dag=dag,
        name='load-bronze',
        image=IMAGE,
        env_vars={**s3_env_vars, **duckdb_env_vars},
        arguments=[
            "--command", "load_data",
            "--input_file_key", FILTERED_FILE_KEY,
            "--table_name", RAW_TABLE_NAME,
            "--schema_name", f"{DATABASE_NAME}.{RAW_SCHEMA}"
        ]
    )

    # Applique les règles de validation définies dans config/validation_rules.py.
    # Les enregistrements valides sont écrits dans data_valid.parquet,
    # les enregistrements invalides dans data_invalid.parquet (quarantaine).
    validate_data = CustomKubernetesPodOperator(
        dag=dag,
        name='validate-data',
        image=IMAGE,
        env_vars={**s3_env_vars, **duckdb_env_vars, **airflow_env_vars},
        arguments=[
            "--command", "validate_data",
            "--input_file_key",   FILTERED_FILE_KEY,
            "--output_file_key",  VALID_FILE_KEY,
            "--invalid_file_key", INVALID_FILE_KEY,
        ]
    )

    # Applique les transformations sur les enregistrements valides :
    # extraction du product_name, construction des URLs d'images,
    # et mise à plat des valeurs nutritionnelles depuis la structure imbriquée.
    transform_data = CustomKubernetesPodOperator(
        dag=dag,
        name='transform-data',
        image=IMAGE,
        env_vars={**s3_env_vars},
        arguments=[
            "--command", "transform_data",
            "--input_file_key",  VALID_FILE_KEY,
            "--output_file_key", TRANSFORMED_FILE_KEY,
        ]
    )

    # Charge les données transformées dans la table staging.source_transformed (couche Silver).
    # Cette table constitue la source principale pour les traitements analytiques futurs.
    load_silver = CustomKubernetesPodOperator(
        dag=dag,
        name='load-silver',
        image=IMAGE,
        env_vars={**s3_env_vars, **duckdb_env_vars},
        arguments=[
            "--command", "load_data",
            "--input_file_key", TRANSFORMED_FILE_KEY,
            "--table_name", STAGING_TABLE_NAME,
            "--schema_name", f"{DATABASE_NAME}.{STAGING_SCHEMA}"
        ]
    )

    # Charge les enregistrements invalides dans staging.source_rejected pour inspection.
    # Ces données peuvent être retraitées une fois les règles de validation ajustées.
    load_rejected = CustomKubernetesPodOperator(
        dag=dag,
        name='load-rejected',
        image=IMAGE,
        env_vars={**s3_env_vars, **duckdb_env_vars},
        arguments=[
            "--command", "load_data",
            "--input_file_key", INVALID_FILE_KEY,
            "--table_name", REJECTED_TABLE_NAME,
            "--schema_name", f"{DATABASE_NAME}.{STAGING_SCHEMA}"
        ]
    )

    end = EmptyOperator(task_id='end')

    start >> create_schemas >> extract_data >> filter_data >> load_bronze >> validate_data >> transform_data >> [load_silver, load_rejected] >> end
