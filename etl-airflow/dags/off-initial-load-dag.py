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

DATABASE_NAME="off"
SCHEMA_NAME="raw"
RAW_TABLE_NAME="raw_canada_products"

with dag:

    start = EmptyOperator(task_id='start')

    create_schema = DuckDBOperator(
        dag=dag,
        task_id='create-schema',
        sql=f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}",
        duckdb_conn_id='duckdb_default'
        )
    
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
    
    load_data = CustomKubernetesPodOperator(
        dag=dag,
        name='load-data',
        image="mig8110/etl-images:1.0.0",
        env_vars={**s3_env_vars, **duckdb_env_vars},
        arguments=[
            "--command", "load_data",
            "--input_file_key", "data.parquet",
            "--table_name", RAW_TABLE_NAME,
            "--schema_name", SCHEMA_NAME
            ]
        )
    
    end = EmptyOperator(task_id='end')

    start >> create_schema >> extract_data >> load_data >> end
