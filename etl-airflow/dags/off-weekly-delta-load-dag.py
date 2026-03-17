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
    dag_id='off_weekly_delta_load',
    default_args=args,
    schedule_interval='@weekly',
    catchup=False,
    tags=['mig8110', 'off', 'delta']
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
SOURCE_TABLE_NAME="canada_products"
DELTA_TABLE_NAME="delta_canada_products"
PRODUCTS_TABLE_NAME="products"
DELTA_FILE_KEY="delta.jsonl"

with dag:

    start = EmptyOperator(task_id='start')

    create_schema = DuckDBOperator(
        dag=dag,
        task_id='create-schema',
        sql=f"CREATE SCHEMA IF NOT EXISTS {DATABASE_NAME}.{SCHEMA_NAME}",
        duckdb_conn_id='duckdb_default'
        )
    
    extract_delta = CustomKubernetesPodOperator(
        dag=dag,
        name='extract-delta',
        image="mig8110/etl-images:1.0.0",
        env_vars={**s3_env_vars},
        arguments=[
            "--command", "extract_delta",
            "--output_file_key", DELTA_FILE_KEY,
            "--url", "https://static.openfoodfacts.org/data/delta/index.txt"
            ]
        )
    
    load_delta = CustomKubernetesPodOperator(
        dag=dag,
        name='load-delta',
        image="mig8110/etl-images:1.0.0",
        env_vars={**s3_env_vars, **duckdb_env_vars},
        arguments=[
            "--command", "load_delta",
            "--input_file_key", DELTA_FILE_KEY,
            "--table_name", DELTA_TABLE_NAME,
            "--schema_name", f"{DATABASE_NAME}.{SCHEMA_NAME}"
            ]
        )

    # TODO: add product_name, images, ingredients, nutriments (incompatible struct types between tables)
    # Rebuild products from full source, then upsert delta
    merge_delta = DuckDBOperator(
        dag=dag,
        task_id='merge-delta',
        sql=f"""
            CREATE OR REPLACE TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{PRODUCTS_TABLE_NAME} AS
                SELECT code, brands, product_quantity, product_quantity_unit,
                       quantity, serving_quantity, serving_size,
                       categories_tags, countries_tags,
                       ecoscore_score, ecoscore_grade,
                       ingredients_tags,
                       nutriscore_score, nutriscore_grade
                FROM {DATABASE_NAME}.{SCHEMA_NAME}.{SOURCE_TABLE_NAME};

            MERGE INTO {DATABASE_NAME}.{SCHEMA_NAME}.{PRODUCTS_TABLE_NAME} AS target
            USING (
                SELECT code, brands, product_quantity, product_quantity_unit,
                       quantity, serving_quantity, serving_size,
                       categories_tags, countries_tags,
                       ecoscore_score, ecoscore_grade,
                       ingredients_tags,
                       nutriscore_score, nutriscore_grade
                FROM {DATABASE_NAME}.{SCHEMA_NAME}.{DELTA_TABLE_NAME}
            ) AS source
            ON target.code = source.code
            WHEN MATCHED THEN UPDATE SET
                brands = source.brands,
                product_quantity = source.product_quantity, product_quantity_unit = source.product_quantity_unit,
                quantity = source.quantity, serving_quantity = source.serving_quantity,
                serving_size = source.serving_size, categories_tags = source.categories_tags,
                countries_tags = source.countries_tags, ecoscore_score = source.ecoscore_score,
                ecoscore_grade = source.ecoscore_grade,
                ingredients_tags = source.ingredients_tags,
                nutriscore_score = source.nutriscore_score, nutriscore_grade = source.nutriscore_grade
            WHEN NOT MATCHED THEN INSERT (
                code, brands, product_quantity, product_quantity_unit,
                quantity, serving_quantity, serving_size,
                categories_tags, countries_tags,
                ecoscore_score, ecoscore_grade,
                ingredients_tags,
                nutriscore_score, nutriscore_grade
            ) VALUES (
                source.code, source.brands, source.product_quantity, source.product_quantity_unit,
                source.quantity, source.serving_quantity, source.serving_size,
                source.categories_tags, source.countries_tags,
                source.ecoscore_score, source.ecoscore_grade,
                source.ingredients_tags,
                source.nutriscore_score, source.nutriscore_grade
            );
        """,
        duckdb_conn_id='duckdb_default'
        )
    
    end = EmptyOperator(task_id='end')

    start >> create_schema >> extract_delta >> load_delta >> merge_delta >> end
