"""
DAG : off_weekly_delta_load
===========================
Chargement incrémental hebdomadaire des produits alimentaires canadiens depuis Open Food Facts.
Ce DAG est déclenché automatiquement chaque semaine et traite uniquement les fichiers delta
publiés depuis la dernière exécution (checkpoint). Il met à jour la table staging.source_transformed
en remplaçant les produits modifiés et en insérant les nouveaux.

Logique de checkpoint :
    - La liste complète des fichiers delta est persistée dans la Variable Airflow `delta_file_list`.
    - Le dernier fichier traité est persisté dans `delta_last_processed_file`.
    - À chaque run, seuls les fichiers postérieurs au checkpoint sont traités.
    - Le checkpoint est mis à jour en fin de run.

Pipeline (par fichier delta) :
    extract_delta    : Télécharge le fichier .json.gz en chunks, filtre par pays, uploade en parquet (Bronze)
    filter_delta     : Sélectionne les colonnes utiles avec fallback pour les champs renommés
    validate_data    : Sépare les enregistrements valides des invalides selon les règles définies
    transform_delta  : Construit les URLs d'images, extrait les nutriments, projette sur le schéma Silver
    load_delta       : Upsert dans MotherDuck (off.staging.source_transformed) — DELETE + INSERT sur code

Outputs S3 (bucket: bi-dev, préfixe: off_weekly_delta_load/delta/) :
    {stem}.parquet             : Enregistrements bruts filtrés par pays (Bronze)
    {stem}_filtered.parquet    : Colonnes sélectionnées
    {stem}_valid.parquet       : Enregistrements valides
    {stem}_invalid.parquet     : Enregistrements invalides (quarantaine)
    {stem}_transformed.parquet : Enregistrements transformés prêts pour le chargement

Output MotherDuck (base: off) :
    staging.source_transformed : Table cible principale — upsert sur `code`
    monitoring.pipeline_runs   : Métriques d'exécution (records_in, records_out, rejection_rate)
"""
import datetime
from airflow.models import DAG, Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from plugins.operators.custom_kubernetes_operator import CustomKubernetesPodOperator

# Image Docker contenant toutes les commandes ETL
IMAGE  = "mig8110/etl-images:1.0.0"
DAG_ID = "off_weekly_delta_load"

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
    max_active_runs=1,
    concurrency=1,
    schedule_interval='@weekly',
    catchup=False,
    tags=['mig8110', 'off', 'delta']
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
STAGING_SCHEMA = "staging"
STAGING_TABLE  = "source_transformed"

# Variables Airflow pour la gestion du checkpoint inter-runs
AIRFLOW_VAR_DELTA_FILE_LIST     = "delta_file_list"
AIRFLOW_VAR_LAST_PROCESSED_FILE = "delta_last_processed_file"

# URL du répertoire delta Open Food Facts
DELTA_INDEX_URL = "https://static.openfoodfacts.org/data/delta/index.txt"
DELTA_BASE_URL  = "https://static.openfoodfacts.org/data/delta/"

# Colonnes à conserver lors du filtrage delta.
# La syntaxe pipe (target|fallback) gère les champs renommés entre versions de fichiers delta :
# si la colonne cible est absente, la colonne de secours est utilisée et renommée.
FILTER_DELTA_COLUMNS = ",".join([
    "code", "brands", "product_name", "product_quantity", "product_quantity_unit",
    "quantity", "serving_quantity", "serving_size", "categories_tags", "countries_tags",
    "ecoscore_score|environmental_score_score",
    "ecoscore_grade|environmental_score_grade",
    "images", "ingredients_tags",
    "nutriscore_score", "nutriscore_grade", "nutriments",
])

# Calcul des fichiers à traiter au parse-time du DAG :
# - Premier run (checkpoint absent) : tous les fichiers de la liste
# - Runs suivants : uniquement les fichiers postérieurs au checkpoint (tri lexicographique = chronologique)
all_delta_files  = Variable.get(AIRFLOW_VAR_DELTA_FILE_LIST, default_var=[], deserialize_json=True)
last_processed   = Variable.get(AIRFLOW_VAR_LAST_PROCESSED_FILE, default_var=None)
files_to_process = [f for f in all_delta_files if f > last_processed] if last_processed else all_delta_files

with dag:

    start = EmptyOperator(task_id='start')

    # Lit index.txt depuis Open Food Facts, trie les fichiers chronologiquement
    # et écrit la liste dans XCom pour la tâche suivante
    fetch_delta_index = CustomKubernetesPodOperator(
        dag=dag,
        name='fetch-delta-index',
        image=IMAGE,
        arguments=[
            "--command", "fetch_delta_index",
            "--url",     DELTA_INDEX_URL,
        ],
        do_xcom_push=True,
    )

    # Persiste la liste des fichiers delta dans une Variable Airflow
    # pour qu'elle soit disponible lors du prochain parse du DAG
    def _save_delta_file_list(ti):
        filenames = ti.xcom_pull(task_ids='fetch-delta-index')
        Variable.set(AIRFLOW_VAR_DELTA_FILE_LIST, filenames, serialize_json=True)

    save_delta_file_list = PythonOperator(
        task_id='save_delta_file_list',
        python_callable=_save_delta_file_list,
        dag=dag,
    )

    # Traitement de chaque fichier delta en parallèle (un TaskGroup par fichier)
    with TaskGroup(group_id='process_delta_files') as process_group:
        for filename in files_to_process:
            # Dérive le préfixe S3 depuis le nom du fichier (sans extension .json.gz)
            stem = filename.replace(".json.gz", "")

            # Clés S3 pour les fichiers intermédiaires de ce fichier delta
            raw_key         = f"{DAG_ID}/delta/{stem}.parquet"
            filtered_key    = f"{DAG_ID}/delta/{stem}_filtered.parquet"
            valid_key       = f"{DAG_ID}/delta/{stem}_valid.parquet"
            invalid_key     = f"{DAG_ID}/delta/{stem}_invalid.parquet"
            transformed_key = f"{DAG_ID}/delta/{stem}_transformed.parquet"

            # Télécharge le fichier .json.gz en chunks de 50 MB, filtre les enregistrements
            # canadiens et sérialise les colonnes complexes en JSON strings (couche Bronze)
            extract = CustomKubernetesPodOperator(
                dag=dag,
                name=f"extract-delta-{stem}",
                task_id=f"extract_delta_{stem}",
                image=IMAGE,
                env_vars={**s3_env_vars},
                arguments=[
                    "--command",        "extract_delta",
                    "--filename",       filename,
                    "--base_url",       DELTA_BASE_URL,
                    "--output_file_key", raw_key,
                ],
            )

            # Sélectionne les colonnes pertinentes avec fallback pour les champs renommés.
            # Les colonnes absentes sont incluses avec None pour garantir un schéma uniforme.
            filter_delta = CustomKubernetesPodOperator(
                dag=dag,
                name=f"filter-delta-{stem}",
                task_id=f"filter_delta_{stem}",
                image=IMAGE,
                env_vars={**s3_env_vars},
                arguments=[
                    "--command",        "filter_delta",
                    "--input_file_key",  raw_key,
                    "--output_file_key", filtered_key,
                    "--columns",         FILTER_DELTA_COLUMNS,
                ],
            )

            # Applique les règles de validation (config/validation_rules.py).
            # Les invalides sont mis en quarantaine dans {stem}_invalid.parquet.
            validate_data = CustomKubernetesPodOperator(
                dag=dag,
                name=f"validate-data-{stem}",
                task_id=f"validate_data_{stem}",
                image=IMAGE,
                env_vars={**s3_env_vars, **duckdb_env_vars, **airflow_env_vars},
                arguments=[
                    "--command",        "validate_data",
                    "--input_file_key",   filtered_key,
                    "--output_file_key",  valid_key,
                    "--invalid_file_key", invalid_key,
                ],
            )

            # Construit les URLs d'images depuis images.selected, extrait les nutriments
            # depuis le dict plat et projette sur le schéma Silver (config/target_columns.py)
            transform_delta = CustomKubernetesPodOperator(
                dag=dag,
                name=f"transform-delta-{stem}",
                task_id=f"transform_delta_{stem}",
                image=IMAGE,
                env_vars={**s3_env_vars},
                arguments=[
                    "--command",        "transform_delta",
                    "--input_file_key",  valid_key,
                    "--output_file_key", transformed_key,
                ],
            )

            # Upsert dans MotherDuck : supprime les lignes dont le code est présent dans
            # le fichier delta, puis insère toutes les lignes (nouveaux + modifiés).
            # Cible : off.staging.source_transformed (même table que le chargement initial)
            load_delta = CustomKubernetesPodOperator(
                dag=dag,
                name=f"load-delta-{stem}",
                task_id=f"load_delta_{stem}",
                image=IMAGE,
                env_vars={**s3_env_vars, **duckdb_env_vars},
                arguments=[
                    "--command",        "load_delta",
                    "--input_file_key", transformed_key,
                    "--table_name",     STAGING_TABLE,
                    "--schema_name",    f"{DATABASE_NAME}.{STAGING_SCHEMA}",
                ],
            )

            extract >> filter_delta >> validate_data >> transform_delta >> load_delta

    # Met à jour le checkpoint avec le dernier fichier traité dans ce run.
    # Au prochain run, seuls les fichiers postérieurs à ce checkpoint seront traités.
    def _update_checkpoint():
        if files_to_process:
            Variable.set(AIRFLOW_VAR_LAST_PROCESSED_FILE, files_to_process[-1])

    update_checkpoint = PythonOperator(
        task_id='update_checkpoint',
        python_callable=_update_checkpoint,
        dag=dag,
    )

    end = EmptyOperator(task_id='end')

    start >> fetch_delta_index >> save_delta_file_list >> process_group >> update_checkpoint >> end
