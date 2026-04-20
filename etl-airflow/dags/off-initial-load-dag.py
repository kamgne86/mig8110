"""
DAG : off_initial_load
======================
Chargement initial complet des produits alimentaires canadiens depuis Open Food Facts.
Ce DAG est déclenché manuellement (schedule_interval=None) et doit être exécuté une seule
fois pour initialiser les données, ou relancé pour un rechargement complet.

Pipeline :
    extract_data           : Télécharge le snapshot parquet depuis GitHub et le dépose sur S3 (Bronze)
    filter_data            : Sélectionne les colonnes utiles + filtre par pays (canada) et langue (lang)
    load_bronze            : Charge le parquet brut (toutes colonnes) dans MotherDuck (off.bronze) — en parallèle avec filter_data
    validate_data          : Sépare les enregistrements valides des invalides selon les règles définies
    transform_data         : Transforme les données : product_name, URLs d'images, nutriments à plat
    normalize_categories   : (parallèle) Normalise categories_tags → categories + ancetre_categories + categorie_principale
    normalize_ingredients  : (parallèle) Normalise ingredients_tags → ingredients + product_ingredients
    finalize_products      : Merge categorie_principale + supprime categories_tags/ingredients_tags → table products finale
    load_products          : Charge silver.products dans MotherDuck
    load_categories        : Charge silver.categories dans MotherDuck
    load_ancetre_categories: Charge silver.ancetre_categories dans MotherDuck
    load_ingredients       : Charge silver.ingredients dans MotherDuck
    load_product_ingredients: Charge silver.product_ingredients dans MotherDuck

Outputs S3 (bucket: bi-dev) :
    Fichier S3                                        Couche    Destination MotherDuck
    ────────────────────────────────────────────────────────────────────────────────────────
    bronze/data.parquet                               Bronze    —  (bronze.products)
    bronze/data_filtered.parquet                      Bronze    —  (transit)
    bronze/data_invalid.parquet                       Bronze    —  (quarantaine)
    silver/data_valid.parquet                         Silver    —  (transit)
    silver/data_transformed.parquet                   Silver    —  (transit)
    silver/categorie_principale.parquet               Silver    —  (transit → mergé dans products)
    silver/products.parquet                           Silver    —  (silver.products)
    silver/categories.parquet                         Silver    —  (silver.categories)
    silver/ancetre_categories.parquet                 Silver    —  (silver.ancetre_categories)
    silver/ingredients.parquet                        Silver    —  (silver.ingredients)
    silver/product_ingredients.parquet                Silver    —  (silver.product_ingredients)

Outputs MotherDuck (base: off) :
    bronze.products               : Données brutes filtrées
    silver.products               : Produits transformés avec categorie_principale (FK → categories)
    silver.categories             : Référentiel OFF des catégories
    silver.ancetre_categories     : Table de fermeture des ancêtres (category_id, category_id_parent, distance)
    silver.ingredients            : Référentiel OFF ingrédients
    silver.product_ingredients    : Jonction Many-to-Many (code, ingredient_id)
    monitoring.pipeline_runs      : Métriques d'exécution (records_in, records_out, rejection_rate)
"""
import pendulum
from airflow.models import DAG
from kubernetes.client import models as k8s
from airflow.operators.empty import EmptyOperator
from plugins.operators.duckdb_operator import DuckDBOperator
from plugins.operators.custom_kubernetes_operator import CustomKubernetesPodOperator


IMAGE  = "mig8110/etl-images:1.0.0"
DAG_ID = "off_initial_load"

# Ressources Kubernetes par type de tâche (cluster : 5 GB RAM, 2 CPU)
RESOURCES_HEAVY = k8s.V1ResourceRequirements(
    requests={"memory": "1Gi",   "cpu": "500m"},
    limits=  {"memory": "3500Mi", "cpu": "1500m"},
)
RESOURCES_MEDIUM = k8s.V1ResourceRequirements(
    requests={"memory": "512Mi", "cpu": "250m"},
    limits=  {"memory": "1500Mi", "cpu": "1000m"},
)
RESOURCES_LIGHT = k8s.V1ResourceRequirements(
    requests={"memory": "256Mi", "cpu": "250m"},
    limits=  {"memory": "1000Mi", "cpu": "500m"},
)

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
    max_active_runs=1,
    concurrency=1,
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

BRONZE_TABLE                  = "products"
SILVER_TABLE                  = "products"
CATEGORIES_TABLE              = "categories"
ANCETRE_CATEGORIES_TABLE      = "ancetre_categories"
INGREDIENTS_TABLE             = "ingredients"
PRODUCT_INGREDIENTS_TABLE     = "product_ingredients"

# Clés S3 préfixées par dag_id pour isoler les fichiers dans le bucket
RAW_FILE_KEY                       = f"{DAG_ID}/bronze/data.parquet"
FILTERED_FILE_KEY                  = f"{DAG_ID}/bronze/data_filtered.parquet"
INVALID_FILE_KEY                   = f"{DAG_ID}/bronze/data_invalid.parquet"
VALID_FILE_KEY                     = f"{DAG_ID}/silver/data_valid.parquet"
TRANSFORMED_FILE_KEY               = f"{DAG_ID}/silver/data_transformed.parquet"
CATEGORIE_PRINCIPALE_FILE_KEY      = f"{DAG_ID}/silver/categorie_principale.parquet"
PRODUCTS_FILE_KEY                  = f"{DAG_ID}/silver/products.parquet"
CATEGORIES_FILE_KEY                = f"{DAG_ID}/silver/categories.parquet"
ANCETRE_CATEGORIES_FILE_KEY        = f"{DAG_ID}/silver/ancetre_categories.parquet"
INGREDIENTS_FILE_KEY               = f"{DAG_ID}/silver/ingredients.parquet"
PRODUCT_INGREDIENTS_FILE_KEY       = f"{DAG_ID}/silver/product_ingredients.parquet"

# Colonnes à conserver lors du filtrage
FILTER_COLUMNS = ",".join([
    "code", "brands", "product_name", "product_quantity", "product_quantity_unit",
    "quantity", "serving_quantity", "serving_size", "categories_tags", "countries_tags",
    "ecoscore_score", "ecoscore_grade", "images", "ingredients_tags", "ingredients_n",
    "ingredients", "nutriscore_score", "nutriscore_grade", "nutriments", "lang",
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
        container_resources=RESOURCES_HEAVY,
        arguments=[
            "--command", "extract_data",
            "--output_file_key", RAW_FILE_KEY,
            "--url", "https://raw.githubusercontent.com/adilblanco/mig8110/main/data/canada_products.parquet.zip"
        ]
    )

    # Sélectionne uniquement les colonnes nécessaires au pipeline,
    # filtre par pays (canada) et par langue (en) pour réduire
    # l'empreinte mémoire des étapes suivantes.
    filter_data = CustomKubernetesPodOperator(
        dag=dag,
        name='filter-data',
        image=IMAGE,
        env_vars={**s3_env_vars},
        container_resources=RESOURCES_HEAVY,
        arguments=[
            "--command",         "filter_data",
            "--input_file_key",  RAW_FILE_KEY,
            "--output_file_key", FILTERED_FILE_KEY,
            "--columns",         FILTER_COLUMNS,
            "--country",         "canada",
            "--lang",            "en",
        ]
    )

    # Charge le parquet brut dans MotherDuck (couche Bronze).
    load_bronze = CustomKubernetesPodOperator(
        dag=dag,
        name='load-bronze',
        image=IMAGE,
        env_vars={**s3_env_vars, **duckdb_env_vars},
        container_resources=RESOURCES_LIGHT,
        arguments=[
            "--command",        "load_data",
            "--input_file_key", RAW_FILE_KEY,
            "--table_name",     BRONZE_TABLE,
            "--schema_name",    f"{DATABASE_NAME}.{BRONZE_SCHEMA}",
        ]
    )

    # Applique les règles de validation définies dans config/validation_rules.py.
    validate_data = CustomKubernetesPodOperator(
        dag=dag,
        name='validate-data',
        image=IMAGE,
        env_vars={**s3_env_vars, **duckdb_env_vars, **airflow_env_vars},
        container_resources=RESOURCES_HEAVY,
        arguments=[
            "--command",          "validate_data",
            "--input_file_key",   FILTERED_FILE_KEY,
            "--output_file_key",  VALID_FILE_KEY,
            "--invalid_file_key", INVALID_FILE_KEY,
            "--schema_name",      MONITORING_SCHEMA,
            "--table_name",       MONITORING_TABLE,
        ]
    )

    # Applique les transformations sur les enregistrements valides.
    transform_data = CustomKubernetesPodOperator(
        dag=dag,
        name='transform-data',
        image=IMAGE,
        env_vars={**s3_env_vars},
        container_resources=RESOURCES_MEDIUM,
        arguments=[
            "--command",         "transform_data",
            "--input_file_key",  VALID_FILE_KEY,
            "--output_file_key", TRANSFORMED_FILE_KEY,
        ]
    )

    # (parallèle) Normalise categories_tags → categories + ancetre_categories
    # + categorie_principale (parquet intermédiaire consommé par finalize_products).
    normalize_categories = CustomKubernetesPodOperator(
        dag=dag,
        name='normalize-categories',
        image=IMAGE,
        env_vars={**s3_env_vars},
        container_resources=RESOURCES_MEDIUM,
        arguments=[
            "--command",                            "normalize_categories",
            "--input_file_key",                     TRANSFORMED_FILE_KEY,
            "--categories_output_key",              CATEGORIES_FILE_KEY,
            "--ancetre_categories_output_key",      ANCETRE_CATEGORIES_FILE_KEY,
            "--categorie_principale_output_key",    CATEGORIE_PRINCIPALE_FILE_KEY,
        ]
    )

    # (parallèle) Normalise ingredients_tags → ingredients + product_ingredients.
    normalize_ingredients = CustomKubernetesPodOperator(
        dag=dag,
        name='normalize-ingredients',
        image=IMAGE,
        env_vars={**s3_env_vars},
        container_resources=RESOURCES_MEDIUM,
        arguments=[
            "--command",                        "normalize_ingredients",
            "--input_file_key",                 TRANSFORMED_FILE_KEY,
            "--ingredients_output_key",         INGREDIENTS_FILE_KEY,
            "--product_ingredients_output_key", PRODUCT_INGREDIENTS_FILE_KEY,
        ]
    )

    # Merge categorie_principale dans products + supprime categories_tags
    # et ingredients_tags → produit la table products finale.
    # Dépend des deux normalize pour garantir que les parquets intermédiaires
    # sont disponibles sur S3 avant d'être consommés.
    finalize_products = CustomKubernetesPodOperator(
        dag=dag,
        name='finalize-products',
        image=IMAGE,
        env_vars={**s3_env_vars},
        container_resources=RESOURCES_LIGHT,
        arguments=[
            "--command",                       "finalize_products",
            "--input_file_key",                TRANSFORMED_FILE_KEY,
            "--categorie_principale_input_key", CATEGORIE_PRINCIPALE_FILE_KEY,
            "--output_file_key",               PRODUCTS_FILE_KEY,
        ]
    )

    # Charge silver.products dans MotherDuck (avec categorie_principale FK).
    load_products = CustomKubernetesPodOperator(
        dag=dag,
        name='load-products',
        image=IMAGE,
        env_vars={**s3_env_vars, **duckdb_env_vars},
        container_resources=RESOURCES_LIGHT,
        arguments=[
            "--command",        "load_data",
            "--input_file_key", PRODUCTS_FILE_KEY,
            "--table_name",     SILVER_TABLE,
            "--schema_name",    f"{DATABASE_NAME}.{SILVER_SCHEMA}",
        ]
    )

    # Charge silver.categories dans MotherDuck (référentiel OFF des catégories).
    # Doit être chargé AVANT ancetre_categories et products car ces deux tables
    # ont des FK vers categories.
    load_categories = CustomKubernetesPodOperator(
        dag=dag,
        name='load-categories',
        image=IMAGE,
        env_vars={**s3_env_vars, **duckdb_env_vars},
        container_resources=RESOURCES_LIGHT,
        arguments=[
            "--command",        "load_data",
            "--input_file_key", CATEGORIES_FILE_KEY,
            "--table_name",     CATEGORIES_TABLE,
            "--schema_name",    f"{DATABASE_NAME}.{SILVER_SCHEMA}",
        ]
    )

    # Charge silver.ancetre_categories dans MotherDuck (closure table).
    # Remplace l'ancienne table product_categories.
    load_ancetre_categories = CustomKubernetesPodOperator(
        dag=dag,
        name='load-ancetre-categories',
        image=IMAGE,
        env_vars={**s3_env_vars, **duckdb_env_vars},
        container_resources=RESOURCES_LIGHT,
        arguments=[
            "--command",        "load_data",
            "--input_file_key", ANCETRE_CATEGORIES_FILE_KEY,
            "--table_name",     ANCETRE_CATEGORIES_TABLE,
            "--schema_name",    f"{DATABASE_NAME}.{SILVER_SCHEMA}",
        ]
    )

    # Charge silver.ingredients dans MotherDuck (référentiel OFF ingrédients).
    load_ingredients = CustomKubernetesPodOperator(
        dag=dag,
        name='load-ingredients',
        image=IMAGE,
        env_vars={**s3_env_vars, **duckdb_env_vars},
        container_resources=RESOURCES_LIGHT,
        arguments=[
            "--command",        "load_data",
            "--input_file_key", INGREDIENTS_FILE_KEY,
            "--table_name",     INGREDIENTS_TABLE,
            "--schema_name",    f"{DATABASE_NAME}.{SILVER_SCHEMA}",
        ]
    )

    # Charge silver.product_ingredients dans MotherDuck (jonction Many-to-Many).
    load_product_ingredients = CustomKubernetesPodOperator(
        dag=dag,
        name='load-product-ingredients',
        image=IMAGE,
        env_vars={**s3_env_vars, **duckdb_env_vars},
        container_resources=RESOURCES_LIGHT,
        arguments=[
            "--command",        "load_data",
            "--input_file_key", PRODUCT_INGREDIENTS_FILE_KEY,
            "--table_name",     PRODUCT_INGREDIENTS_TABLE,
            "--schema_name",    f"{DATABASE_NAME}.{SILVER_SCHEMA}",
        ]
    )

    end = EmptyOperator(task_id='end')

    # ── Dépendances ──────────────────────────────────────────────────────────
    start >> create_schemas >> extract_data
    extract_data >> load_bronze >> end
    extract_data >> filter_data >> validate_data >> transform_data

    # normalize_categories et normalize_ingredients tournent en parallèle
    transform_data >> [normalize_categories, normalize_ingredients]

    # finalize_products attend les deux normalize :
    # - normalize_categories pour categorie_principale.parquet
    # - normalize_ingredients pour garantir que le parquet transformé n'est plus utilisé
    [normalize_categories, normalize_ingredients] >> finalize_products

    # categories doit être chargé avant ancetre_categories et products (FK)
    finalize_products >> load_categories
    load_categories >> [load_ancetre_categories, load_products]
    finalize_products >> [load_ingredients, load_product_ingredients]

    [load_products, load_categories, load_ancetre_categories,
     load_ingredients, load_product_ingredients] >> end
    