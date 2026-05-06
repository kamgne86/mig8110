"""
DAG : off_weekly_delta_load
===========================
Chargement incrémental hebdomadaire des produits alimentaires canadiens depuis Open Food Facts.

Logique de checkpoint :
    - `delta_file_list`           : liste complète des fichiers delta (Variable Airflow)
    - `delta_last_processed_file` : dernier fichier traité (Variable Airflow)

    Cas 1 — 1ère exécution (Variables vides) :
        pending = toute la liste → pipeline_per_file se map sur tous les fichiers

    Cas 2 — Exécutions suivantes, nouveaux fichiers détectés :
        pending = fichiers postérieurs au checkpoint → pipeline_per_file se map sur le delta

    Cas 3 — Aucun nouveau fichier :
        pending = [] → check_new_files branche directement vers end (tâches en rose/skipped)

Architecture — Mapped Task Groups (Airflow 2.5+) :
    Combine @task_group et expand_kwargs() pour créer un groupe de tâches par fichier delta
    au runtime. Chaque groupe contient 5 tâches ETL avec des logs séparés dans l'UI Airflow
    (contrairement à @task + expand() où tous les logs sont concaténés dans une seule instance).
    concurrency=1 garantit le traitement séquentiel (1 fichier à la fois).

    Le corps d'un @task_group s'exécute au parse-time pour définir la structure du DAG —
    les arguments reçus sont des MappedArgument impossibles à transformer avec du code Python.
    Solution : build_file_params (PythonOperator) pré-calcule toutes les chaînes dérivées
    (clés S3, noms de pods) et retourne une liste de dicts plats. expand_kwargs() distribue
    ensuite chaque dict comme ensemble de paramètres nommés à une instance du groupe.

Pipeline (par fichier delta, 1 task group instance par fichier) :
    extract_delta          : Télécharge le fichier .json.gz, filtre par pays, uploade en parquet (Bronze)
    filter_delta           : Sélectionne les colonnes utiles avec fallback pour les champs renommés + filtre par langue (en)
    validate_delta         : Sépare les enregistrements valides des invalides selon les règles définies
    transform_delta        : Construit les URLs d'images, extrait les nutriments, projette sur le schéma Silver
    normalize_categories   : (parallèle) Normalise categories_tags → categories + ancetre_categories + categorie_principale
    normalize_ingredients  : (parallèle) Normalise ingredients → ingredients + product_ingredients + sous_ingredients + ingredient_alias
    finalize_products      : Merge categorie_principale + supprime categories_tags/ingredients → table products finale
    load_products          : Upsert silver.products — DELETE + INSERT par code
    load_categories        : Upsert silver.categories — DELETE + INSERT par category_name
    load_ancetre_categories: Upsert silver.ancetre_categories — DELETE + INSERT par (category_id, category_id_parent)
    load_ingredients       : Upsert silver.ingredients — DELETE + INSERT par ingredient_id
    load_product_ingredients: Upsert silver.product_ingredients — DELETE + INSERT par code
    load_sous_ingredients  : Upsert silver.sous_ingredients — DELETE + INSERT par code
    load_ingredient_alias  : Upsert silver.ingredient_alias — DELETE + INSERT par ingredient_id

Outputs S3 (bucket: bi-dev) :
    Fichier S3                                                Couche    Destination MotherDuck
    ──────────────────────────────────────────────────────────────────────────────────────────────
    bronze/{stem}.parquet                                     Bronze    —  (transit)
    bronze/{stem}_filtered.parquet                            Bronze    —  (transit)
    bronze/{stem}_invalid.parquet                             Bronze    —  (quarantaine)
    silver/{stem}_valid.parquet                               Silver    —  (transit)
    silver/{stem}_transformed.parquet                         Silver    —  (transit)
    silver/{stem}_categorie_principale.parquet                Silver    —  (transit → mergé dans products)
    silver/{stem}_products.parquet                            Silver    —  (silver.products)
    silver/{stem}_categories.parquet                          Silver    —  (silver.categories)
    silver/{stem}_ancetre_categories.parquet                  Silver    —  (silver.ancetre_categories)
    silver/{stem}_ingredients.parquet                         Silver    —  (silver.ingredients)
    silver/{stem}_product_ingredients.parquet                 Silver    —  (silver.product_ingredients)
    silver/{stem}_sous_ingredients.parquet                    Silver    —  (silver.sous_ingredients)
    silver/{stem}_ingredient_alias.parquet                    Silver    —  (silver.ingredient_alias)

Output MotherDuck (base: off) :
    silver.products               : Produits transformés avec categorie_principale — upsert sur `code`
    silver.categories             : Référentiel OFF des catégories — upsert sur `category_name`
    silver.ancetre_categories     : Table de fermeture des ancêtres — upsert sur `(category_id, category_id_parent)`
    silver.ingredients            : Référentiel OFF ingrédients — upsert sur `ingredient_id`
    silver.product_ingredients    : Jonction produit ↔ ingrédient niveau 1 — upsert sur `code`
    silver.sous_ingredients       : Composition des ingrédients composés niveau 2+ — upsert sur `code`
    silver.ingredient_alias       : Variantes textuelles d'un ingrédient — upsert sur `ingredient_id`
    monitoring.pipeline_runs      : Métriques d'exécution (records_in, records_out, rejection_rate)
"""

import re
import pendulum
from airflow.decorators import task_group
from airflow.models import DAG, Variable
from kubernetes.client import models as k8s
from airflow.models.xcom_arg import XComArg
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from plugins.operators.custom_kubernetes_operator import CustomKubernetesPodOperator
from plugins.operators.duckdb_operator import DuckDBOperator

IMAGE  = "mig8110/etl-images:1.0.0"
DAG_ID = "off_weekly_delta_load"

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
    schedule_interval='@weekly',
    catchup=False,
    tags=['mig8110', 'off', 'delta'],
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

DATABASE_NAME                = "off"
SILVER_SCHEMA                = "silver"
SILVER_TABLE                 = "products"
CATEGORIES_TABLE             = "categories"
ANCETRE_CATEGORIES_TABLE     = "ancetre_categories"
INGREDIENTS_TABLE            = "ingredients"
PRODUCT_INGREDIENTS_TABLE    = "product_ingredients"
SOUS_INGREDIENTS_TABLE       = "sous_ingredients"
INGREDIENT_ALIAS_TABLE       = "ingredient_alias"
MONITORING_SCHEMA            = "monitoring"
MONITORING_TABLE             = "pipeline_runs"

AIRFLOW_VAR_DELTA_FILE_LIST     = "delta_file_list"
AIRFLOW_VAR_LAST_PROCESSED_FILE = "delta_last_processed_file"

DELTA_INDEX_URL = "https://static.openfoodfacts.org/data/delta/index.txt"
DELTA_BASE_URL  = "https://static.openfoodfacts.org/data/delta/"

# Colonnes à conserver lors du filtrage delta.
# La syntaxe pipe (target|fallback) gère les champs renommés entre versions de fichiers delta :
# si la colonne cible est absente, la colonne de secours est utilisée et renommée.
# Note: "nutrition" est utilisé à la place de "nutriments" (présent mais vide dans les deltas récents).
FILTER_DELTA_COLUMNS = ",".join([
    "code", "brands", "product_name", "product_quantity", "product_quantity_unit",
    "quantity", "serving_quantity", "serving_size", "categories_tags", "countries_tags",
    "ecoscore_score|environmental_score_score", "ecoscore_grade|environmental_score_grade",
    "images", "ingredients", "ingredients_n", "nutriscore_score", "nutriscore_grade", "nutrition", "lang",
])


with dag:

    start = EmptyOperator(task_id='start')

    # ── 0. Schemas ───────────────────────────────────────────────────────────
    # Crée les schémas Bronze, Silver et Monitoring dans MotherDuck si absents.
    create_schemas = DuckDBOperator(
        dag=dag,
        task_id='create-schemas',
        sql=f"""
            CREATE SCHEMA IF NOT EXISTS {DATABASE_NAME}.{SILVER_SCHEMA};
            CREATE SCHEMA IF NOT EXISTS {DATABASE_NAME}.{MONITORING_SCHEMA};
        """,
        duckdb_conn_id='duckdb_default'
    )

    # ── 1. Fetch ─────────────────────────────────────────────────────────────
    # Lit index.txt depuis Open Food Facts, trie les fichiers chronologiquement
    # et pousse la liste dans XCom via do_xcom_push=True.
    fetch_delta_index = CustomKubernetesPodOperator(
        dag=dag,
        name='fetch-delta-index',
        task_id='fetch_delta_index',
        image=IMAGE,
        arguments=[
            "--command", "fetch_delta_index",
            "--url",     DELTA_INDEX_URL,
        ],
        do_xcom_push=True,
    )

    # ── 2. Save + calcul des pending ─────────────────────────────────────────
    # Retourne les fichiers à traiter : tous si pas de checkpoint, sinon ceux postérieurs.
    def _pending_files(all_files, last_file):
        return [f for f in all_files if f > last_file] if last_file else all_files

    # Persiste la liste complète dans la Variable Airflow ET retourne les fichiers
    # pending via XCom pour alimenter build_file_params à l'étape suivante.
    #
    # cas 1 — Variables vides    : last_file=None  → pending = toute la liste
    # cas 2 — Nouveaux fichiers  : last_file=X     → pending = fichiers > X
    # cas 3 — Rien de nouveau    : last_file=X     → pending = []
    def _save_delta_file_list(ti) -> list:
        all_files = ti.xcom_pull(task_ids='fetch_delta_index') or []
        last_file = Variable.get(AIRFLOW_VAR_LAST_PROCESSED_FILE, default_var=None)

        Variable.set(AIRFLOW_VAR_DELTA_FILE_LIST, all_files, serialize_json=True)

        pending = _pending_files(all_files, last_file)
        print(f"[save_delta_file_list] total={len(all_files)} | last='{last_file}' | pending={len(pending)}")
        return pending

    save_delta_file_list = PythonOperator(
        task_id='save_delta_file_list',
        python_callable=_save_delta_file_list,
        dag=dag,
    )

    # ── 3. Branchement ───────────────────────────────────────────────────────
    # Lit le XCom de save_delta_file_list (liste des fichiers pending).
    # S'il y a des fichiers à traiter → build_file_params (puis pipeline_per_file).
    # Sinon → end (toutes les tâches de traitement apparaissent en rose/skipped dans l'UI).
    def _check_new_files(ti):
        pending = ti.xcom_pull(task_ids='save_delta_file_list') or []
        print(f"[check_new_files] {len(pending)} fichier(s) en attente")
        return 'build_file_params' if pending else 'end'

    check_new_files = BranchPythonOperator(
        task_id='check_new_files',
        python_callable=_check_new_files,
        dag=dag,
    )

    # ── 4. Pré-calcul des paramètres par fichier ─────────────────────────────
    # Le corps d'un @task_group s'exécute au parse-time : les arguments reçus via
    # expand_kwargs() sont des MappedArgument — impossibles à transformer avec du
    # code Python (str.replace, re.findall, f-strings...).
    # Ce PythonOperator calcule à l'avance toutes les chaînes dérivées (clés S3,
    # noms de pods) pour chaque fichier pending et retourne une liste de dicts plats.
    # expand_kwargs() distribue ensuite chaque dict comme paramètres nommés à une
    # instance du groupe pipeline_per_file.
    def _build_file_params(ti) -> list:
        pending = ti.xcom_pull(task_ids='save_delta_file_list') or []
        result = []
        for s in pending:
            sk = s.replace('.json.gz', '')
            ps = '-'.join(re.findall(r'\d+', s)[-2:])
            result.append({
                'stem':                            s,
                'extract_name':                    f'extract-delta-{ps}',
                'filter_name':                     f'filter-delta-{ps}',
                'validate_name':                   f'validate-delta-{ps}',
                'transform_name':                  f'transform-delta-{ps}',
                'normalize_categories_name':           f'normalize-categories-{ps}',
                'normalize_ingredients_name':          f'normalize-ingredients-{ps}',
                'finalize_products_name':              f'finalize-products-{ps}',
                'load_products_name':                  f'load-products-{ps}',
                'load_categories_name':                f'load-categories-{ps}',
                'load_ancetre_categories_name':        f'load-ancetre-categories-{ps}',
                'load_ingredients_name':               f'load-ingredients-{ps}',
                'load_product_ingredients_name':       f'load-product-ingredients-{ps}',
                'load_sous_ingredients_name':          f'load-sous-ingredients-{ps}',
                'load_ingredient_alias_name':          f'load-ingredient-alias-{ps}',
                'raw_key':                             f'{DAG_ID}/bronze/{sk}.parquet',
                'filtered_key':                        f'{DAG_ID}/bronze/{sk}_filtered.parquet',
                'invalid_key':                         f'{DAG_ID}/bronze/{sk}_invalid.parquet',
                'valid_key':                           f'{DAG_ID}/silver/{sk}_valid.parquet',
                'transformed_key':                     f'{DAG_ID}/silver/{sk}_transformed.parquet',
                'categorie_principale_key':            f'{DAG_ID}/silver/{sk}_categorie_principale.parquet',
                'products_key':                        f'{DAG_ID}/silver/{sk}_products.parquet',
                'categories_key':                      f'{DAG_ID}/silver/{sk}_categories.parquet',
                'ancetre_categories_key':              f'{DAG_ID}/silver/{sk}_ancetre_categories.parquet',
                'ingredients_key':                     f'{DAG_ID}/silver/{sk}_ingredients.parquet',
                'product_ingredients_key':             f'{DAG_ID}/silver/{sk}_product_ingredients.parquet',
                'sous_ingredients_key':                f'{DAG_ID}/silver/{sk}_sous_ingredients.parquet',
                'ingredient_alias_key':                f'{DAG_ID}/silver/{sk}_ingredient_alias.parquet',
            })
        return result

    build_file_params = PythonOperator(
        task_id='build_file_params',
        python_callable=_build_file_params,
        dag=dag,
    )

    # ── 5. Mapped Task Groups — 1 groupe de tâches par fichier ──────────────
    # Technique : @task_group + expand_kwargs() (Airflow 2.5+) — un groupe de
    # 5 tâches ETL (CustomKubernetesPodOperator) est instancié au runtime pour
    # chaque fichier delta. Chaque étape a ses propres logs dans l'UI Airflow.
    # Les templates Jinja (conn.*) sont rendus par Airflow car on utilise des
    # opérateurs classiques.
    @task_group(group_id='pipeline_per_file')
    def pipeline_per_file(stem, extract_name, filter_name, validate_name, transform_name,
                          normalize_categories_name, normalize_ingredients_name, finalize_products_name,
                          load_products_name, load_categories_name, load_ancetre_categories_name,
                          load_ingredients_name, load_product_ingredients_name,
                          load_sous_ingredients_name, load_ingredient_alias_name,
                          raw_key, filtered_key, valid_key, invalid_key, transformed_key,
                          categorie_principale_key, products_key, categories_key, ancetre_categories_key,
                          ingredients_key, product_ingredients_key, sous_ingredients_key, ingredient_alias_key):

        # ── extract_delta ────────────────────────────────────────────────────
        # Télécharge le fichier .json.gz en chunks, filtre les enregistrements
        # canadiens et sérialise les colonnes complexes en JSON strings (Bronze).
        extract = CustomKubernetesPodOperator(
            dag=dag,
            task_id='extract_delta',
            name=extract_name,
            image=IMAGE,
            arguments=[
                "--command",         "extract_delta",
                "--filename",        stem,
                "--base_url",        DELTA_BASE_URL,
                "--output_file_key", raw_key,
                "--columns",         FILTER_DELTA_COLUMNS,
            ],
            env_vars={**s3_env_vars, **airflow_env_vars},
            container_resources=RESOURCES_HEAVY,
            do_xcom_push=False,
        )

        # ── filter_delta ─────────────────────────────────────────────────────
        # Sélectionne les colonnes pertinentes avec fallback pour les champs renommés.
        # Les colonnes absentes sont incluses avec None pour garantir un schéma uniforme.
        # Filtre par langue (en) — le filtre pays est déjà appliqué dans extract_delta.
        filter_ = CustomKubernetesPodOperator(
            dag=dag,
            task_id='filter_delta',
            name=filter_name,
            image=IMAGE,
            arguments=[
                "--command",         "filter_delta",
                "--input_file_key",  raw_key,
                "--output_file_key", filtered_key,
                "--columns",         FILTER_DELTA_COLUMNS,
                "--lang",            "en",
            ],
            env_vars={**s3_env_vars},
            container_resources=RESOURCES_MEDIUM,
            do_xcom_push=False,
        )

        # ── validate_delta ───────────────────────────────────────────────────
        # Applique les règles de validation delta (config/validation_rules_delta.py).
        # Les invalides sont mis en quarantaine dans {stem_key}_invalid.parquet.
        validate = CustomKubernetesPodOperator(
            dag=dag,
            task_id='validate_delta',
            name=validate_name,
            image=IMAGE,
            arguments=[
                "--command",          "validate_delta",
                "--input_file_key",   filtered_key,
                "--output_file_key",  valid_key,
                "--invalid_file_key", invalid_key,
                "--schema_name",      MONITORING_SCHEMA,
                "--table_name",       MONITORING_TABLE,
            ],
            env_vars={**s3_env_vars, **duckdb_env_vars, **airflow_env_vars},
            container_resources=RESOURCES_MEDIUM,
            do_xcom_push=False,
        )

        # ── transform_delta ──────────────────────────────────────────────────
        # Construit les URLs d'images, extrait les nutriments depuis le dict plat
        # et projette sur le schéma Silver (config/target_columns.py).
        transform = CustomKubernetesPodOperator(
            dag=dag,
            task_id='transform_delta',
            name=transform_name,
            image=IMAGE,
            arguments=[
                "--command",         "transform_delta",
                "--input_file_key",  valid_key,
                "--output_file_key", transformed_key,
            ],
            env_vars={**s3_env_vars},
            container_resources=RESOURCES_MEDIUM,
            do_xcom_push=False,
        )

        # ── normalize_categories ─────────────────────────────────────────────
        # (parallèle) Normalise categories_tags → categories + ancetre_categories
        # + categorie_principale (parquet intermédiaire consommé par finalize_products).
        normalize_categories = CustomKubernetesPodOperator(
            dag=dag,
            task_id='normalize_categories',
            name=normalize_categories_name,
            image=IMAGE,
            arguments=[
                "--command",                          "normalize_categories",
                "--input_file_key",                   transformed_key,
                "--categories_output_key",            categories_key,
                "--ancetre_categories_output_key",    ancetre_categories_key,
                "--categorie_principale_output_key",  categorie_principale_key,
            ],
            env_vars={**s3_env_vars},
            container_resources=RESOURCES_MEDIUM,
            do_xcom_push=False,
        )

        # ── normalize_ingredients ────────────────────────────────────────────
        # (parallèle) Normalise ingredients → ingredients + product_ingredients
        # + sous_ingredients + ingredient_alias.
        normalize_ingredients = CustomKubernetesPodOperator(
            dag=dag,
            task_id='normalize_ingredients',
            name=normalize_ingredients_name,
            image=IMAGE,
            arguments=[
                "--command",                          "normalize_ingredients",
                "--input_file_key",                   transformed_key,
                "--ingredients_output_key",           ingredients_key,
                "--product_ingredients_output_key",   product_ingredients_key,
                "--sous_ingredients_output_key",      sous_ingredients_key,
                "--ingredient_alias_output_key",      ingredient_alias_key,
            ],
            env_vars={**s3_env_vars},
            container_resources=RESOURCES_MEDIUM,
            do_xcom_push=False,
        )

        # ── finalize_products ────────────────────────────────────────────────
        # Merge categorie_principale dans products + supprime categories_tags
        # et ingredients_tags → produit la table products finale.
        # Dépend des deux normalize pour garantir que les parquets intermédiaires
        # sont disponibles sur S3 avant d'être consommés.
        finalize_products = CustomKubernetesPodOperator(
            dag=dag,
            task_id='finalize_products',
            name=finalize_products_name,
            image=IMAGE,
            arguments=[
                "--command",                        "finalize_products",
                "--input_file_key",                 transformed_key,
                "--categorie_principale_input_key", categorie_principale_key,
                "--output_file_key",                products_key,
            ],
            env_vars={**s3_env_vars},
            container_resources=RESOURCES_LIGHT,
            do_xcom_push=False,
        )

        # ── load_products ────────────────────────────────────────────────────
        # Upsert silver.products — DELETE + INSERT par code.
        load_products = CustomKubernetesPodOperator(
            dag=dag,
            task_id='load_products',
            name=load_products_name,
            image=IMAGE,
            arguments=[
                "--command",        "load_delta",
                "--input_file_key", products_key,
                "--table_name",     SILVER_TABLE,
                "--schema_name",    f"{DATABASE_NAME}.{SILVER_SCHEMA}",
            ],
            env_vars={**s3_env_vars, **duckdb_env_vars},
            container_resources=RESOURCES_LIGHT,
            do_xcom_push=False,
        )

        # ── load_categories ──────────────────────────────────────────────────
        # Upsert silver.categories — DELETE + INSERT par category_name.
        # Doit être chargé AVANT ancetre_categories (FK).
        load_categories = CustomKubernetesPodOperator(
            dag=dag,
            task_id='load_categories',
            name=load_categories_name,
            image=IMAGE,
            arguments=[
                "--command",        "load_delta",
                "--input_file_key", categories_key,
                "--table_name",     CATEGORIES_TABLE,
                "--schema_name",    f"{DATABASE_NAME}.{SILVER_SCHEMA}",
                "--key_column",     "category_name",
            ],
            env_vars={**s3_env_vars, **duckdb_env_vars},
            container_resources=RESOURCES_LIGHT,
            do_xcom_push=False,
        )

        # ── load_ancetre_categories ──────────────────────────────────────────
        # Upsert silver.ancetre_categories — DELETE + INSERT par (category_id, category_id_parent).
        # Remplace l'ancienne table product_categories.
        load_ancetre_categories = CustomKubernetesPodOperator(
            dag=dag,
            task_id='load_ancetre_categories',
            name=load_ancetre_categories_name,
            image=IMAGE,
            arguments=[
                "--command",        "load_delta",
                "--input_file_key", ancetre_categories_key,
                "--table_name",     ANCETRE_CATEGORIES_TABLE,
                "--schema_name",    f"{DATABASE_NAME}.{SILVER_SCHEMA}",
                "--key_column",     "category_id",
                "--key_column2",    "category_id_parent",
            ],
            env_vars={**s3_env_vars, **duckdb_env_vars},
            container_resources=RESOURCES_LIGHT,
            do_xcom_push=False,
        )

        # ── load_ingredients ─────────────────────────────────────────────────
        # Upsert silver.ingredients — DELETE + INSERT par ingredient_id (hash stable).
        # Doit être chargé AVANT product_ingredients, sous_ingredients et ingredient_alias (FK).
        load_ingredients = CustomKubernetesPodOperator(
            dag=dag,
            task_id='load_ingredients',
            name=load_ingredients_name,
            image=IMAGE,
            arguments=[
                "--command",        "load_delta",
                "--input_file_key", ingredients_key,
                "--table_name",     INGREDIENTS_TABLE,
                "--schema_name",    f"{DATABASE_NAME}.{SILVER_SCHEMA}",
                "--key_column",     "ingredient_id",
            ],
            env_vars={**s3_env_vars, **duckdb_env_vars},
            container_resources=RESOURCES_LIGHT,
            do_xcom_push=False,
        )

        # ── load_product_ingredients ─────────────────────────────────────────
        # Upsert silver.product_ingredients — DELETE + INSERT par code.
        load_product_ingredients = CustomKubernetesPodOperator(
            dag=dag,
            task_id='load_product_ingredients',
            name=load_product_ingredients_name,
            image=IMAGE,
            arguments=[
                "--command",        "load_delta",
                "--input_file_key", product_ingredients_key,
                "--table_name",     PRODUCT_INGREDIENTS_TABLE,
                "--schema_name",    f"{DATABASE_NAME}.{SILVER_SCHEMA}",
                "--key_column",     "code",
            ],
            env_vars={**s3_env_vars, **duckdb_env_vars},
            container_resources=RESOURCES_LIGHT,
            do_xcom_push=False,
        )

        # ── load_sous_ingredients ────────────────────────────────────────────
        # Upsert silver.sous_ingredients — DELETE + INSERT par code.
        load_sous_ingredients = CustomKubernetesPodOperator(
            dag=dag,
            task_id='load_sous_ingredients',
            name=load_sous_ingredients_name,
            image=IMAGE,
            arguments=[
                "--command",        "load_delta",
                "--input_file_key", sous_ingredients_key,
                "--table_name",     SOUS_INGREDIENTS_TABLE,
                "--schema_name",    f"{DATABASE_NAME}.{SILVER_SCHEMA}",
                "--key_column",     "code",
            ],
            env_vars={**s3_env_vars, **duckdb_env_vars},
            container_resources=RESOURCES_LIGHT,
            do_xcom_push=False,
        )

        # ── load_ingredient_alias ────────────────────────────────────────────
        # Upsert silver.ingredient_alias — DELETE + INSERT par ingredient_id.
        load_ingredient_alias = CustomKubernetesPodOperator(
            dag=dag,
            task_id='load_ingredient_alias',
            name=load_ingredient_alias_name,
            image=IMAGE,
            arguments=[
                "--command",        "load_delta",
                "--input_file_key", ingredient_alias_key,
                "--table_name",     INGREDIENT_ALIAS_TABLE,
                "--schema_name",    f"{DATABASE_NAME}.{SILVER_SCHEMA}",
                "--key_column",     "ingredient_id",
            ],
            env_vars={**s3_env_vars, **duckdb_env_vars},
            container_resources=RESOURCES_LIGHT,
            do_xcom_push=False,
        )

        extract >> filter_ >> validate >> transform
        transform >> [normalize_categories, normalize_ingredients]
        [normalize_categories, normalize_ingredients] >> finalize_products
        # categories doit être chargé avant ancetre_categories et products (FK)
        finalize_products >> load_categories
        load_categories >> [load_ancetre_categories, load_products]
        # ingredients doit être chargé avant product_ingredients, sous_ingredients et ingredient_alias (FK)
        finalize_products >> load_ingredients
        load_ingredients >> [load_product_ingredients, load_sous_ingredients, load_ingredient_alias]

    process_mapped = pipeline_per_file.expand_kwargs(XComArg(build_file_params))

    # ── 6. Checkpoint ────────────────────────────────────────────────────────
    # Persiste le dernier fichier traité dans la Variable Airflow pour le prochain run.
    # Lit le XCom de save_delta_file_list au runtime — toujours cohérent avec ce qui
    # a été effectivement traité dans ce run.
    def _update_checkpoint(ti):
        pending = ti.xcom_pull(task_ids='save_delta_file_list') or []
        if pending:
            Variable.set(AIRFLOW_VAR_LAST_PROCESSED_FILE, pending[-1])
            print(f"[update_checkpoint] checkpoint → '{pending[-1]}'")

    update_checkpoint = PythonOperator(
        task_id='update_checkpoint',
        python_callable=_update_checkpoint,
        dag=dag,
    )

    # ── 7. Fin ───────────────────────────────────────────────────────────────
    # trigger_rule NONE_FAILED_MIN_ONE_SUCCESS :
    #   - Pipeline exécuté avec échec → end échoue → DAG en rouge
    #   - Aucun nouveau fichier (skip direct) → tâches en skipped, pas failed
    #     → end passe quand même (skipped ≠ failed)
    end = EmptyOperator(
        task_id='end',
        trigger_rule='none_failed_min_one_success',
        dag=dag,
    )

    # ── Dépendances ──────────────────────────────────────────────────────────
    # check_new_files >> build_file_params : dépendance explicite pour que
    # BranchPythonOperator skipe build_file_params (et tout l'aval) quand
    # pending=[]. La dépendance XCom save_delta_file_list→build_file_params
    # est implicite (ti.xcom_pull dans _build_file_params).
    start >> create_schemas >> fetch_delta_index >> save_delta_file_list >> check_new_files
    check_new_files >> build_file_params >> process_mapped >> update_checkpoint >> end
    check_new_files >> end
