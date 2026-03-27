import datetime
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from plugins.operators.duckdb_operator import DuckDBOperator
from plugins.operators.custom_kubernetes_operator import CustomKubernetesPodOperator


args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2026, 1, 1),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=60)
}

dag = DAG(
    dag_id='off_initial_load',
    default_args=args,
    schedule_interval=None,
    catchup=False,
    tags=['mig8110', 'off']
)

s3_env_vars = {
    "S3_ENDPOINT": "{{ conn.s3_conn.host }}",
    "S3_ACCESS_KEY": "{{ conn.s3_conn.login }}",
    "S3_SECRET_KEY": "{{ conn.s3_conn.password }}",
    "S3_BUCKET": "{{ conn.s3_conn.schema }}",
    }

duckdb_env_vars = {
    "DUCKDB_TOKEN": "{{ conn.duckdb_default.password }}",
    "DUCKDB_DB": "{{ conn.duckdb_default.schema }}",
    }

DATABASE_NAME = "off"
RAW_SCHEMA    = "raw"
STAGING_SCHEMA = "staging"

RAW_TABLE_NAME         = "canada_products"
STAGING_TABLE_NAME     = "source_transformed"
REJECTED_TABLE_NAME    = "source_rejected"

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
        image="mig8110/etl-images:1.0.0",
        env_vars={**s3_env_vars},
        arguments=[
            "--command", "extract_data",
            "--output_file_key", "data.parquet",
            "--url", "https://raw.githubusercontent.com/adilblanco/mig8110/main/data/canada_products.parquet.zip"
            ]
        )

    load_bronze = CustomKubernetesPodOperator(
        dag=dag,
        name='load-bronze',
        image="mig8110/etl-images:1.0.0",
        env_vars={**s3_env_vars, **duckdb_env_vars},
        arguments=[
            "--command", "load_data",
            "--input_file_key", "data.parquet",
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
        image="mig8110/etl-images:1.0.0",
        env_vars={**s3_env_vars},
        arguments=[
            "--command", "validate_data",
            "--input_file_key",   "data.parquet",
            "--output_file_key",  "data_valid.parquet",
            "--invalid_file_key", "data_invalid.parquet"
            ]
        )

    # Applique les transformations sur les enregistrements valides :
    # extraction du product_name, construction des URLs d'images,
    # et mise à plat des valeurs nutritionnelles depuis la structure imbriquée.
    transform_data = CustomKubernetesPodOperator(
        dag=dag,
        name='transform-data',
        image="mig8110/etl-images:1.0.0",
        env_vars={**s3_env_vars},
        arguments=[
            "--command", "transform_data",
            "--input_file_key",  "data_valid.parquet",
            "--output_file_key", "data_transformed.parquet"
            ]
        )

    # Charge les données transformées dans la table staging.source_transformed (couche Silver).
    # Cette table constitue la source principale pour les traitements analytiques futurs.
    load_silver = CustomKubernetesPodOperator(
        dag=dag,
        name='load-silver',
        image="mig8110/etl-images:1.0.0",
        env_vars={**s3_env_vars, **duckdb_env_vars},
        arguments=[
            "--command", "load_data",
            "--input_file_key", "data_transformed.parquet",
            "--table_name", STAGING_TABLE_NAME,
            "--schema_name", f"{DATABASE_NAME}.{STAGING_SCHEMA}"
            ]
        )

    # Charge les enregistrements invalides dans staging.source_rejected pour inspection.
    # Ces données peuvent être retraitées une fois les règles de validation ajustées.
    load_rejected = CustomKubernetesPodOperator(
        dag=dag,
        name='load-rejected',
        image="mig8110/etl-images:1.0.0",
        env_vars={**s3_env_vars, **duckdb_env_vars},
        arguments=[
            "--command", "load_data",
            "--input_file_key", "data_invalid.parquet",
            "--table_name", REJECTED_TABLE_NAME,
            "--schema_name", f"{DATABASE_NAME}.{STAGING_SCHEMA}"
            ]
        )

    end = EmptyOperator(task_id='end')

    start >> create_schemas >> extract_data >> load_bronze >> validate_data >> transform_data >> [load_silver, load_rejected] >> end
