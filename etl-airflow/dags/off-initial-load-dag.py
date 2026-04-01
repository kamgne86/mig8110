"""
DAG : off_initial_load
======================
Chargement initial complet des produits alimentaires canadiens depuis Open Food Facts.
Ce DAG est déclenché manuellement (schedule_interval=None) et doit être exécuté une seule
fois pour initialiser les données, ou relancé pour un rechargement complet.

Pipeline :
    extract_data   : Télécharge le snapshot parquet depuis GitHub et le dépose sur S3 (Bronze)
    filter_data    : Sélectionne les colonnes utiles pour réduire l'empreinte mémoire
    load_bronze    : Charge le parquet filtré dans MotherDuck (off.bronze)
    validate_data  : Sépare les enregistrements valides des invalides selon les règles définies
    transform_data : Transforme les données : product_name, URLs d'images, nutriments à plat
    load_silver    : Charge les données transformées dans MotherDuck (off.silver)

Outputs S3 (bucket: bi-dev) :
    Fichier S3                              Couche    Destination MotherDuck
    ──────────────────────────────────────────────────────────────────────────
    bronze/data.parquet                     Bronze    —  (transit)
    bronze/data_filtered.parquet            Bronze    —  (bronze.products)
    bronze/data_invalid.parquet             Bronze    —  (quarantaine)
    silver/data_valid.parquet               Silver    —  (transit)
    silver/data_transformed.parquet         Silver    —  (silver.products)

Outputs MotherDuck (base: off) :
    bronze.products          : Données brutes filtrées
    silver.products          : Données transformées — source principale pour l'application
    monitoring.pipeline_runs : Métriques d'exécution (records_in, records_out, rejection_rate)
"""
import pendulum
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from plugins.operators.duckdb_operator import DuckDBOperator
from plugins.operators.custom_kubernetes_operator import CustomKubernetesPodOperator


IMAGE  = "mig8110/etl-images:1.0.0"
DAG_ID = "off_initial_load"

args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2026, 1, 1, tz="America/Montreal"),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=60),
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=args,
    schedule_interval=None,
    catchup=False,
    tags=['mig8110', 'off']
)

# Connexions Airflow — rendues par Jinja au moment de l'exécution des opérateurs classiques.
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
    "AIRFLOW_CTX_DAG_ID":     "{{ dag.dag_id }}",
}

DATABASE_NAME      = "off"
BRONZE_SCHEMA      = "bronze"
SILVER_SCHEMA      = "silver"
MONITORING_SCHEMA  = "monitoring"
MONITORING_TABLE   = "pipeline_runs"

BRONZE_TABLE = "products"
SILVER_TABLE = "products"

# Clés S3 préfixées par dag_id pour isoler les fichiers dans le bucket
RAW_FILE_KEY         = f"{DAG_ID}/bronze/data.parquet"
FILTERED_FILE_KEY    = f"{DAG_ID}/bronze/data_filtered.parquet"
INVALID_FILE_KEY     = f"{DAG_ID}/bronze/data_invalid.parquet"
VALID_FILE_KEY       = f"{DAG_ID}/silver/data_valid.parquet"
TRANSFORMED_FILE_KEY = f"{DAG_ID}/silver/data_transformed.parquet"

# Colonnes à conserver lors du filtrage
FILTER_COLUMNS = ",".join([
    "code", "brands", "product_name", "product_quantity", "product_quantity_unit",
    "quantity", "serving_quantity", "serving_size", "categories_tags", "countries_tags",
    "ecoscore_score", "ecoscore_grade", "images", "ingredients_tags", "ingredients",
    "nutriscore_score", "nutriscore_grade", "nutriments",
])

with dag:

    start = EmptyOperator(task_id='start')

    # Crée les schémas Bronze, Silver et Monitoring dans MotherDuck si absents.
    create_schemas = DuckDBOperator(
        dag=dag,
        task_id='create-schemas',
        sql=f"""
            CREATE SCHEMA IF NOT EXISTS {DATABASE_NAME}.{BRONZE_SCHEMA};
            CREATE SCHEMA IF NOT EXISTS {DATABASE_NAME}.{SILVER_SCHEMA};
            CREATE SCHEMA IF NOT EXISTS {DATABASE_NAME}.{MONITORING_SCHEMA};
        """,
        duckdb_conn_id='duckdb_default'
    )

    # Télécharge le snapshot initial des produits canadiens depuis GitHub
    # et le dépose sur S3 au format parquet brut (couche Bronze).
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
    # afin de réduire l'empreinte mémoire des étapes suivantes.
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

    # Charge le parquet filtré dans MotherDuck (couche Bronze).
    load_bronze = CustomKubernetesPodOperator(
        dag=dag,
        name='load-bronze',
        image=IMAGE,
        env_vars={**s3_env_vars, **duckdb_env_vars},
        arguments=[
            "--command", "load_data",
            "--input_file_key", FILTERED_FILE_KEY,
            "--table_name", BRONZE_TABLE,
            "--schema_name", f"{DATABASE_NAME}.{BRONZE_SCHEMA}",
        ]
    )

    # Applique les règles de validation définies dans config/validation_rules.py.
    # Les enregistrements valides sont écrits dans data_valid.parquet,
    # les enregistrements invalides dans data_invalid.parquet (quarantaine S3).
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
            "--schema_name",      MONITORING_SCHEMA,
            "--table_name",       MONITORING_TABLE,
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

    # Charge les données transformées dans silver.products (couche Silver).
    # Cette table constitue la source principale pour les traitements analytiques futurs.
    load_silver = CustomKubernetesPodOperator(
        dag=dag,
        name='load-silver',
        image=IMAGE,
        env_vars={**s3_env_vars, **duckdb_env_vars},
        arguments=[
            "--command", "load_data",
            "--input_file_key", TRANSFORMED_FILE_KEY,
            "--table_name", SILVER_TABLE,
            "--schema_name", f"{DATABASE_NAME}.{SILVER_SCHEMA}",
        ]
    )

    end = EmptyOperator(task_id='end')

    start >> create_schemas >> extract_data >> filter_data >> load_bronze >> validate_data >> transform_data >> load_silver >> end
