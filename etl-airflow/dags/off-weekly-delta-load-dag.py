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

    # Transform images: add URL columns to canada_products
    transform_images_source = DuckDBOperator(
        dag=dag,
        task_id='transform-images-source',
        sql=f"""
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{SOURCE_TABLE_NAME} ADD COLUMN IF NOT EXISTS front_url VARCHAR;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{SOURCE_TABLE_NAME} ADD COLUMN IF NOT EXISTS ingredients_url VARCHAR;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{SOURCE_TABLE_NAME} ADD COLUMN IF NOT EXISTS nutrition_url VARCHAR;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{SOURCE_TABLE_NAME} ADD COLUMN IF NOT EXISTS packaging_url VARCHAR;

            UPDATE {DATABASE_NAME}.{SCHEMA_NAME}.{SOURCE_TABLE_NAME} SET
                front_url = 'https://images.openfoodfacts.org/images/products/' ||
                    substr(lpad(code, 13, '0'), 1, 3) || '/' || substr(lpad(code, 13, '0'), 4, 3) || '/' ||
                    substr(lpad(code, 13, '0'), 7, 3) || '/' || substr(lpad(code, 13, '0'), 10, 4) ||
                    '/front_en.' || CAST(list_filter(images, x -> x.key = 'front_en')[1].rev AS INTEGER) || '.400.jpg',
                ingredients_url = 'https://images.openfoodfacts.org/images/products/' ||
                    substr(lpad(code, 13, '0'), 1, 3) || '/' || substr(lpad(code, 13, '0'), 4, 3) || '/' ||
                    substr(lpad(code, 13, '0'), 7, 3) || '/' || substr(lpad(code, 13, '0'), 10, 4) ||
                    '/ingredients_en.' || CAST(list_filter(images, x -> x.key = 'ingredients_en')[1].rev AS INTEGER) || '.400.jpg',
                nutrition_url = 'https://images.openfoodfacts.org/images/products/' ||
                    substr(lpad(code, 13, '0'), 1, 3) || '/' || substr(lpad(code, 13, '0'), 4, 3) || '/' ||
                    substr(lpad(code, 13, '0'), 7, 3) || '/' || substr(lpad(code, 13, '0'), 10, 4) ||
                    '/nutrition_en.' || CAST(list_filter(images, x -> x.key = 'nutrition_en')[1].rev AS INTEGER) || '.400.jpg',
                packaging_url = 'https://images.openfoodfacts.org/images/products/' ||
                    substr(lpad(code, 13, '0'), 1, 3) || '/' || substr(lpad(code, 13, '0'), 4, 3) || '/' ||
                    substr(lpad(code, 13, '0'), 7, 3) || '/' || substr(lpad(code, 13, '0'), 10, 4) ||
                    '/packaging_en.' || CAST(list_filter(images, x -> x.key = 'packaging_en')[1].rev AS INTEGER) || '.400.jpg';
        """,
        duckdb_conn_id='duckdb_default'
        )

    # Transform images: add URL columns to delta_canada_products
    transform_images_delta = DuckDBOperator(
        dag=dag,
        task_id='transform-images-delta',
        sql=f"""
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{DELTA_TABLE_NAME} ADD COLUMN IF NOT EXISTS front_url VARCHAR;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{DELTA_TABLE_NAME} ADD COLUMN IF NOT EXISTS ingredients_url VARCHAR;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{DELTA_TABLE_NAME} ADD COLUMN IF NOT EXISTS nutrition_url VARCHAR;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{DELTA_TABLE_NAME} ADD COLUMN IF NOT EXISTS packaging_url VARCHAR;

            UPDATE {DATABASE_NAME}.{SCHEMA_NAME}.{DELTA_TABLE_NAME} SET
                front_url = 'https://images.openfoodfacts.org/images/products/' ||
                    substr(lpad(code, 13, '0'), 1, 3) || '/' || substr(lpad(code, 13, '0'), 4, 3) || '/' ||
                    substr(lpad(code, 13, '0'), 7, 3) || '/' || substr(lpad(code, 13, '0'), 10, 4) ||
                    '/front_en.' || REPLACE(images.selected.front.en.rev, '"', '') || '.400.jpg',
                ingredients_url = 'https://images.openfoodfacts.org/images/products/' ||
                    substr(lpad(code, 13, '0'), 1, 3) || '/' || substr(lpad(code, 13, '0'), 4, 3) || '/' ||
                    substr(lpad(code, 13, '0'), 7, 3) || '/' || substr(lpad(code, 13, '0'), 10, 4) ||
                    '/ingredients_en.' || REPLACE(images.selected.ingredients.en.rev, '"', '') || '.400.jpg',
                nutrition_url = 'https://images.openfoodfacts.org/images/products/' ||
                    substr(lpad(code, 13, '0'), 1, 3) || '/' || substr(lpad(code, 13, '0'), 4, 3) || '/' ||
                    substr(lpad(code, 13, '0'), 7, 3) || '/' || substr(lpad(code, 13, '0'), 10, 4) ||
                    '/nutrition_en.' || REPLACE(images.selected.nutrition.en.rev, '"', '') || '.400.jpg',
                packaging_url = 'https://images.openfoodfacts.org/images/products/' ||
                    substr(lpad(code, 13, '0'), 1, 3) || '/' || substr(lpad(code, 13, '0'), 4, 3) || '/' ||
                    substr(lpad(code, 13, '0'), 7, 3) || '/' || substr(lpad(code, 13, '0'), 10, 4) ||
                    '/packaging_en.' || REPLACE(images.selected.packaging.en.rev, '"', '') || '.400.jpg';
        """,
        duckdb_conn_id='duckdb_default'
        )

    # Transform nutriments: extract 14 nutriment values from canada_products
    transform_nutriments_source = DuckDBOperator(
        dag=dag,
        task_id='transform-nutriments-source',
        sql=f"""
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{SOURCE_TABLE_NAME} ADD COLUMN IF NOT EXISTS energy_kcal_100g DOUBLE;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{SOURCE_TABLE_NAME} ADD COLUMN IF NOT EXISTS fat_100g DOUBLE;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{SOURCE_TABLE_NAME} ADD COLUMN IF NOT EXISTS saturated_fat_100g DOUBLE;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{SOURCE_TABLE_NAME} ADD COLUMN IF NOT EXISTS trans_fat_100g DOUBLE;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{SOURCE_TABLE_NAME} ADD COLUMN IF NOT EXISTS cholesterol_100g DOUBLE;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{SOURCE_TABLE_NAME} ADD COLUMN IF NOT EXISTS sodium_100g DOUBLE;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{SOURCE_TABLE_NAME} ADD COLUMN IF NOT EXISTS salt_100g DOUBLE;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{SOURCE_TABLE_NAME} ADD COLUMN IF NOT EXISTS carbohydrates_100g DOUBLE;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{SOURCE_TABLE_NAME} ADD COLUMN IF NOT EXISTS fiber_100g DOUBLE;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{SOURCE_TABLE_NAME} ADD COLUMN IF NOT EXISTS sugars_100g DOUBLE;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{SOURCE_TABLE_NAME} ADD COLUMN IF NOT EXISTS proteins_100g DOUBLE;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{SOURCE_TABLE_NAME} ADD COLUMN IF NOT EXISTS calcium_100g DOUBLE;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{SOURCE_TABLE_NAME} ADD COLUMN IF NOT EXISTS iron_100g DOUBLE;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{SOURCE_TABLE_NAME} ADD COLUMN IF NOT EXISTS potassium_100g DOUBLE;

            UPDATE {DATABASE_NAME}.{SCHEMA_NAME}.{SOURCE_TABLE_NAME} SET
                energy_kcal_100g = list_filter(nutriments, x -> x.name = 'energy-kcal')[1]."100g",
                fat_100g = list_filter(nutriments, x -> x.name = 'fat')[1]."100g",
                saturated_fat_100g = list_filter(nutriments, x -> x.name = 'saturated-fat')[1]."100g",
                trans_fat_100g = list_filter(nutriments, x -> x.name = 'trans-fat')[1]."100g",
                cholesterol_100g = list_filter(nutriments, x -> x.name = 'cholesterol')[1]."100g",
                sodium_100g = list_filter(nutriments, x -> x.name = 'sodium')[1]."100g",
                salt_100g = list_filter(nutriments, x -> x.name = 'salt')[1]."100g",
                carbohydrates_100g = list_filter(nutriments, x -> x.name = 'carbohydrates')[1]."100g",
                fiber_100g = list_filter(nutriments, x -> x.name = 'fiber')[1]."100g",
                sugars_100g = list_filter(nutriments, x -> x.name = 'sugars')[1]."100g",
                proteins_100g = list_filter(nutriments, x -> x.name = 'proteins')[1]."100g",
                calcium_100g = list_filter(nutriments, x -> x.name = 'calcium')[1]."100g",
                iron_100g = list_filter(nutriments, x -> x.name = 'iron')[1]."100g",
                potassium_100g = list_filter(nutriments, x -> x.name = 'potassium')[1]."100g";
        """,
        duckdb_conn_id='duckdb_default'
        )

    # Transform nutriments: extract 14 nutriment values from delta_canada_products
    transform_nutriments_delta = DuckDBOperator(
        dag=dag,
        task_id='transform-nutriments-delta',
        sql=f"""
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{DELTA_TABLE_NAME} ADD COLUMN IF NOT EXISTS energy_kcal_100g DOUBLE;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{DELTA_TABLE_NAME} ADD COLUMN IF NOT EXISTS fat_100g DOUBLE;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{DELTA_TABLE_NAME} ADD COLUMN IF NOT EXISTS saturated_fat_100g DOUBLE;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{DELTA_TABLE_NAME} ADD COLUMN IF NOT EXISTS trans_fat_100g DOUBLE;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{DELTA_TABLE_NAME} ADD COLUMN IF NOT EXISTS cholesterol_100g DOUBLE;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{DELTA_TABLE_NAME} ADD COLUMN IF NOT EXISTS sodium_100g DOUBLE;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{DELTA_TABLE_NAME} ADD COLUMN IF NOT EXISTS salt_100g DOUBLE;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{DELTA_TABLE_NAME} ADD COLUMN IF NOT EXISTS carbohydrates_100g DOUBLE;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{DELTA_TABLE_NAME} ADD COLUMN IF NOT EXISTS fiber_100g DOUBLE;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{DELTA_TABLE_NAME} ADD COLUMN IF NOT EXISTS sugars_100g DOUBLE;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{DELTA_TABLE_NAME} ADD COLUMN IF NOT EXISTS proteins_100g DOUBLE;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{DELTA_TABLE_NAME} ADD COLUMN IF NOT EXISTS calcium_100g DOUBLE;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{DELTA_TABLE_NAME} ADD COLUMN IF NOT EXISTS iron_100g DOUBLE;
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{DELTA_TABLE_NAME} ADD COLUMN IF NOT EXISTS potassium_100g DOUBLE;

            UPDATE {DATABASE_NAME}.{SCHEMA_NAME}.{DELTA_TABLE_NAME} SET
                energy_kcal_100g = nutriments."energy-kcal_100g",
                fat_100g = nutriments.fat_100g,
                saturated_fat_100g = nutriments."saturated-fat_100g",
                trans_fat_100g = nutriments."trans-fat_100g",
                cholesterol_100g = nutriments.cholesterol_100g,
                sodium_100g = nutriments.sodium_100g,
                salt_100g = nutriments.salt_100g,
                carbohydrates_100g = nutriments.carbohydrates_100g,
                fiber_100g = nutriments.fiber_100g,
                sugars_100g = nutriments.sugars_100g,
                proteins_100g = nutriments.proteins_100g,
                calcium_100g = nutriments.calcium_100g,
                iron_100g = nutriments.iron_100g,
                potassium_100g = nutriments.potassium_100g;
        """,
        duckdb_conn_id='duckdb_default'
        )

    # Transform product_name: extract main text from canada_products (delta is already VARCHAR)
    transform_product_name = DuckDBOperator(
        dag=dag,
        task_id='transform-product-name',
        sql=f"""
            ALTER TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{SOURCE_TABLE_NAME} ADD COLUMN IF NOT EXISTS product_name_text VARCHAR;
            UPDATE {DATABASE_NAME}.{SCHEMA_NAME}.{SOURCE_TABLE_NAME} SET
                product_name_text = list_filter(product_name, x -> x.lang = 'main')[1].text;
        """,
        duckdb_conn_id='duckdb_default'
        )

    # Upsert: update existing products by code, insert new ones
    merge_delta = DuckDBOperator(
        dag=dag,
        task_id='merge-delta',
        sql=f"""
            CREATE OR REPLACE TABLE {DATABASE_NAME}.{SCHEMA_NAME}.{PRODUCTS_TABLE_NAME} AS
                SELECT code, product_name_text AS product_name, brands, product_quantity, product_quantity_unit,
                       quantity, serving_quantity, serving_size,
                       categories_tags, countries_tags,
                       ecoscore_score, ecoscore_grade,
                       ingredients_tags,
                       nutriscore_score, nutriscore_grade,
                       front_url, ingredients_url, nutrition_url, packaging_url,
                       energy_kcal_100g, fat_100g, saturated_fat_100g, trans_fat_100g,
                       cholesterol_100g, sodium_100g, salt_100g, carbohydrates_100g,
                       fiber_100g, sugars_100g, proteins_100g,
                       calcium_100g, iron_100g, potassium_100g
                FROM {DATABASE_NAME}.{SCHEMA_NAME}.{SOURCE_TABLE_NAME};

            MERGE INTO {DATABASE_NAME}.{SCHEMA_NAME}.{PRODUCTS_TABLE_NAME} AS target
            USING (
                SELECT code, product_name, brands, product_quantity, product_quantity_unit,
                       quantity, serving_quantity, serving_size,
                       categories_tags, countries_tags,
                       ecoscore_score, ecoscore_grade,
                       ingredients_tags,
                       nutriscore_score, nutriscore_grade,
                       front_url, ingredients_url, nutrition_url, packaging_url,
                       energy_kcal_100g, fat_100g, saturated_fat_100g, trans_fat_100g,
                       cholesterol_100g, sodium_100g, salt_100g, carbohydrates_100g,
                       fiber_100g, sugars_100g, proteins_100g,
                       calcium_100g, iron_100g, potassium_100g
                FROM {DATABASE_NAME}.{SCHEMA_NAME}.{DELTA_TABLE_NAME}
            ) AS source
            ON target.code = source.code
            WHEN MATCHED THEN UPDATE SET
                product_name = source.product_name,
                brands = source.brands,
                product_quantity = source.product_quantity, product_quantity_unit = source.product_quantity_unit,
                quantity = source.quantity, serving_quantity = source.serving_quantity,
                serving_size = source.serving_size, categories_tags = source.categories_tags,
                countries_tags = source.countries_tags, ecoscore_score = source.ecoscore_score,
                ecoscore_grade = source.ecoscore_grade,
                ingredients_tags = source.ingredients_tags,
                nutriscore_score = source.nutriscore_score, nutriscore_grade = source.nutriscore_grade,
                front_url = source.front_url, ingredients_url = source.ingredients_url,
                nutrition_url = source.nutrition_url, packaging_url = source.packaging_url,
                energy_kcal_100g = source.energy_kcal_100g, fat_100g = source.fat_100g,
                saturated_fat_100g = source.saturated_fat_100g, trans_fat_100g = source.trans_fat_100g,
                cholesterol_100g = source.cholesterol_100g, sodium_100g = source.sodium_100g,
                salt_100g = source.salt_100g, carbohydrates_100g = source.carbohydrates_100g,
                fiber_100g = source.fiber_100g, sugars_100g = source.sugars_100g,
                proteins_100g = source.proteins_100g,
                calcium_100g = source.calcium_100g, iron_100g = source.iron_100g,
                potassium_100g = source.potassium_100g
            WHEN NOT MATCHED THEN INSERT (
                code, product_name, brands, product_quantity, product_quantity_unit,
                quantity, serving_quantity, serving_size,
                categories_tags, countries_tags,
                ecoscore_score, ecoscore_grade,
                ingredients_tags,
                nutriscore_score, nutriscore_grade,
                front_url, ingredients_url, nutrition_url, packaging_url,
                energy_kcal_100g, fat_100g, saturated_fat_100g, trans_fat_100g,
                cholesterol_100g, sodium_100g, salt_100g, carbohydrates_100g,
                fiber_100g, sugars_100g, proteins_100g,
                calcium_100g, iron_100g, potassium_100g
            ) VALUES (
                source.code, source.product_name, source.brands, source.product_quantity, source.product_quantity_unit,
                source.quantity, source.serving_quantity, source.serving_size,
                source.categories_tags, source.countries_tags,
                source.ecoscore_score, source.ecoscore_grade,
                source.ingredients_tags,
                source.nutriscore_score, source.nutriscore_grade,
                source.front_url, source.ingredients_url, source.nutrition_url, source.packaging_url,
                source.energy_kcal_100g, source.fat_100g, source.saturated_fat_100g, source.trans_fat_100g,
                source.cholesterol_100g, source.sodium_100g, source.salt_100g, source.carbohydrates_100g,
                source.fiber_100g, source.sugars_100g, source.proteins_100g,
                source.calcium_100g, source.iron_100g, source.potassium_100g
            );
        """,
        duckdb_conn_id='duckdb_default'
        )
    
    end = EmptyOperator(task_id='end')

    start >> create_schema >> extract_delta >> load_delta >> [transform_images_source, transform_images_delta, transform_product_name, transform_nutriments_source, transform_nutriments_delta] >> merge_delta >> end
