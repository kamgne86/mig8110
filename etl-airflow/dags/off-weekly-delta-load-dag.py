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
    - Toutes les décisions (branching, checkpoint) sont prises au runtime après que
      `save_delta_file_list` a mis à jour les Variables — jamais au parse-time.
    - Le BranchPythonOperator `check_new_files` décide au moment de l'exécution :
        * s'il y a de nouveaux fichiers → branche vers `process_delta_files` (tâches en vert)
        * sinon                         → branche directement vers `end` (tâches en rose/skipped)
    - Le checkpoint est mis à jour en fin de run uniquement si des fichiers ont été traités.

Premier run (Variables absentes) :
    Le TaskGroup est généré au parse-time depuis `delta_file_list`. Si la Variable est absente,
    le TaskGroup est vide mais `check_new_files` branche quand même vers `process_delta_files`
    dès que `save_delta_file_list` l'a remplie — car la décision est prise au runtime.
    Le TaskGroup vide ne pose pas de problème : Airflow re-parse le DAG (30s–2min) et les
    tâches apparaissent au run suivant. En production (@weekly), ce n'est pas un problème.

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
from airflow.operators.python import PythonOperator, BranchPythonOperator
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


def _pending_files(all_files, last_file):
    """Retourne les fichiers à traiter : tous si pas de checkpoint, sinon ceux postérieurs au checkpoint."""
    return [f for f in all_files if f > last_file] if last_file else all_files


# Génération du TaskGroup au parse-time : seuls les fichiers pending sont générés,
# pour que toutes les tâches du groupe correspondent exactement aux fichiers à traiter.
_all_files          = Variable.get(AIRFLOW_VAR_DELTA_FILE_LIST, default_var=[], deserialize_json=True)
_last_processed     = Variable.get(AIRFLOW_VAR_LAST_PROCESSED_FILE, default_var=None)
files_for_taskgroup = _pending_files(_all_files, _last_processed)

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
    # pour qu'elle soit disponible au prochain parse du DAG
    def _save_delta_file_list(ti):
        filenames = ti.xcom_pull(task_ids='fetch-delta-index')
        Variable.set(AIRFLOW_VAR_DELTA_FILE_LIST, filenames, serialize_json=True)

    save_delta_file_list = PythonOperator(
        task_id='save_delta_file_list',
        python_callable=_save_delta_file_list,
        dag=dag,
    )

    # Compare la liste des fichiers delta avec le checkpoint au runtime (après save_delta_file_list).
    # Branche vers `process_delta_files` s'il y a de nouveaux fichiers (tâches en vert),
    # ou directement vers `end` si tout est déjà à jour (tâches en rose/skipped).
    def _check_new_files():
        current_files = Variable.get(AIRFLOW_VAR_DELTA_FILE_LIST, default_var=[], deserialize_json=True)
        current_last  = Variable.get(AIRFLOW_VAR_LAST_PROCESSED_FILE, default_var=None)
        if _pending_files(current_files, current_last):
            return 'process_delta_files'
        return 'end'

    check_new_files = BranchPythonOperator(
        task_id='check_new_files',
        python_callable=_check_new_files,
        dag=dag,
    )

    # TODO: remplacer par le TaskGroup avec les vrais CustomKubernetesPodOperator
    #       une fois le circuit (branching, skip, checkpoint) validé.
    process_group = EmptyOperator(task_id='process_delta_files', dag=dag)

    # Met à jour le checkpoint avec le dernier fichier traité dans ce run.
    # Lit les Variables au runtime pour refléter l'état après save_delta_file_list.
    def _update_checkpoint():
        current_files = Variable.get(AIRFLOW_VAR_DELTA_FILE_LIST, default_var=[], deserialize_json=True)
        current_last  = Variable.get(AIRFLOW_VAR_LAST_PROCESSED_FILE, default_var=None)
        pending = _pending_files(current_files, current_last)
        if pending:
            Variable.set(AIRFLOW_VAR_LAST_PROCESSED_FILE, pending[-1])

    update_checkpoint = PythonOperator(
        task_id='update_checkpoint',
        python_callable=_update_checkpoint,
        dag=dag,
    )

    # trigger_rule ALL_DONE : end s'exécute quelle que soit la branche prise
    # (process_delta_files → update_checkpoint en vert, ou skip direct via check_new_files en rose)
    end = EmptyOperator(
        task_id='end',
        trigger_rule='all_done',
        dag=dag,
    )

    start >> fetch_delta_index >> save_delta_file_list >> check_new_files
    check_new_files >> process_group >> update_checkpoint >> end
    check_new_files >> end
