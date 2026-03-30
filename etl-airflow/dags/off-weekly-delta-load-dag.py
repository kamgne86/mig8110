"""
DAG : off_weekly_delta_load
===========================
Chargement incrémental hebdomadaire des produits alimentaires canadiens depuis Open Food Facts.

Logique de checkpoint :
    - `delta_file_list`           : liste complète des fichiers delta (Variable Airflow)
    - `delta_last_processed_file` : dernier fichier traité (Variable Airflow)

    Cas 1 — 1ère exécution (Variables vides) :
        pending = toute la liste → process_delta_file se map sur tous les fichiers

    Cas 2 — Exécutions suivantes, nouveaux fichiers détectés :
        pending = fichiers postérieurs au checkpoint → process_delta_file se map sur le delta

    Cas 3 — Aucun nouveau fichier :
        pending = [] → check_new_files branche directement vers end (tâches en rose/skipped)

Architecture — Dynamic Task Mapping (Airflow 2.3+) :
    Remplace le TaskGroup dynamique (génération parse-time) par expand() (génération runtime).
    Une task instance par fichier, créée au moment de l'exécution à partir du XCom.
    concurrency=1 garantit le traitement séquentiel (1 fichier à la fois).

Pipeline (par fichier delta, 1 task instance par fichier) :
    extract_delta  : Télécharge le fichier .json.gz, filtre par pays, uploade en parquet (Bronze)
    filter_delta   : Sélectionne les colonnes utiles avec fallback pour les champs renommés
    validate_data  : Sépare les enregistrements valides des invalides selon les règles définies
    transform_delta: Construit les URLs d'images, extrait les nutriments, projette sur le schéma Silver
    load_delta     : Upsert atomique dans MotherDuck (off.staging.source_transformed) — DELETE + INSERT dans une transaction (ROLLBACK si INSERT échoue)

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

import re
import datetime
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models import DAG, Variable
from airflow.models.xcom_arg import XComArg
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from plugins.operators.custom_kubernetes_operator import CustomKubernetesPodOperator

IMAGE  = "mig8110/etl-images:1.0.0"
DAG_ID = "off_weekly_delta_load"

args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2026, 1, 1),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=60),
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

# Connexions — utilisées uniquement par les opérateurs classiques (Jinja rendu par Airflow).
# À l'intérieur d'un @task, les templates ne sont pas rendus : on utilise BaseHook à la place.
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

DATABASE_NAME  = "off"
STAGING_SCHEMA = "staging"
STAGING_TABLE  = "source_transformed"

AIRFLOW_VAR_DELTA_FILE_LIST     = "delta_file_list"
AIRFLOW_VAR_LAST_PROCESSED_FILE = "delta_last_processed_file"

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
    """Retourne les fichiers à traiter : tous si pas de checkpoint, sinon ceux postérieurs."""
    return [f for f in all_files if f > last_file] if last_file else all_files


with dag:

    start = EmptyOperator(task_id='start')

    # ── 1. Fetch ─────────────────────────────────────────────────────────────
    # Lit index.txt depuis Open Food Facts, trie les fichiers chronologiquement
    # et pousse la liste dans XCom via do_xcom_push=True.
    fetch_delta_index = CustomKubernetesPodOperator(
        dag=dag,
        name='fetch_delta_index',
        task_id='fetch_delta_index',
        image=IMAGE,
        arguments=[
            "--command", "fetch_delta_index",
            "--url",     DELTA_INDEX_URL,
        ],
        do_xcom_push=True,
    )

    # ── 2. Save + calcul des pending ─────────────────────────────────────────
    # Persiste la liste complète dans la Variable Airflow ET retourne les fichiers
    # pending via XCom pour alimenter expand() à l'étape suivante.
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
    # S'il y a des fichiers à traiter → process_delta_file (expand crée 1 instance par fichier).
    # Sinon → end (toutes les tâches de traitement apparaissent en rose/skipped dans l'UI).
    def _check_new_files(ti):
        pending = ti.xcom_pull(task_ids='save_delta_file_list') or []
        print(f"[check_new_files] {len(pending)} fichier(s) en attente")
        return 'process_delta_file' if pending else 'end'

    check_new_files = BranchPythonOperator(
        task_id='check_new_files',
        python_callable=_check_new_files,
        dag=dag,
    )

    # ── 4. Dynamic Task Mapping — 1 task instance par fichier ───────────────
    # Technique : @task + expand() (Airflow 2.3+) — les instances sont créées au
    # runtime depuis le XCom de save_delta_file_list, contrairement au TaskGroup
    # qui génère les tâches statiquement au parse-time.
    # Chaque instance exécute séquentiellement les 5 étapes ETL pour un fichier delta.
    # Les connexions sont résolues via BaseHook car les templates Jinja ne sont pas
    # rendus à l'intérieur d'un @task.
    @task(task_id='process_delta_file', dag=dag)
    def process_delta_file(stem: str, **context):

        # Résolution des connexions au runtime
        s3_conn     = BaseHook.get_connection('s3_conn')
        duckdb_conn = BaseHook.get_connection('duckdb_default')

        resolved_s3_env_vars = {
            "S3_ENDPOINT":   s3_conn.host,
            "S3_ACCESS_KEY": s3_conn.login,
            "S3_SECRET_KEY": s3_conn.password,
            "S3_BUCKET":     s3_conn.schema,
        }
        resolved_duckdb_env_vars = {
            "DUCKDB_TOKEN": duckdb_conn.password,
            "DUCKDB_DB":    duckdb_conn.schema,
        }
        resolved_airflow_env_vars = {
            "AIRFLOW_CTX_DAG_RUN_ID": context.get('run_id', ''),
        }

        # Dérive les clés S3 et le suffixe du nom de pod depuis le nom de fichier.
        # pod_suffix : 2 derniers groupes de chiffres (timestamps), ex: 1774687161-1774770293
        stem_key   = stem.replace('.json.gz', '')
        pod_suffix = '-'.join(re.findall(r'\d+', stem)[-2:])

        # ── extract_delta ────────────────────────────────────────────────────
        # Télécharge le fichier .json.gz en chunks, filtre les enregistrements
        # canadiens et sérialise les colonnes complexes en JSON strings (Bronze).
        CustomKubernetesPodOperator(
            dag=dag,
            task_id='extract_delta_pod',
            name=f'extract-delta-{pod_suffix}',
            image=IMAGE,
            arguments=[
                "--command",         "extract_delta",
                "--filename",        stem,
                "--base_url",        DELTA_BASE_URL,
                "--output_file_key", f"{DAG_ID}/delta/{stem_key}.parquet",
            ],
            env_vars={**resolved_s3_env_vars, **resolved_airflow_env_vars},
            do_xcom_push=False,
        ).execute(context=context)

        # ── filter_delta ─────────────────────────────────────────────────────
        # Sélectionne les colonnes pertinentes avec fallback pour les champs renommés.
        # Les colonnes absentes sont incluses avec None pour garantir un schéma uniforme.
        CustomKubernetesPodOperator(
            dag=dag,
            task_id='filter_delta_pod',
            name=f'filter-delta-{pod_suffix}',
            image=IMAGE,
            arguments=[
                "--command",         "filter_delta",
                "--input_file_key",  f"{DAG_ID}/delta/{stem_key}.parquet",
                "--output_file_key", f"{DAG_ID}/delta/{stem_key}_filtered.parquet",
                "--columns",         FILTER_DELTA_COLUMNS,
            ],
            env_vars={**resolved_s3_env_vars},
            do_xcom_push=False,
        ).execute(context=context)

        # ── validate_data ────────────────────────────────────────────────────
        # Applique les règles de validation (config/validation_rules.py).
        # Les invalides sont mis en quarantaine dans {stem_key}_invalid.parquet.
        CustomKubernetesPodOperator(
            dag=dag,
            task_id='validate_data_pod',
            name=f'validate-data-{pod_suffix}',
            image=IMAGE,
            arguments=[
                "--command",          "validate_data",
                "--input_file_key",   f"{DAG_ID}/delta/{stem_key}_filtered.parquet",
                "--output_file_key",  f"{DAG_ID}/delta/{stem_key}_valid.parquet",
                "--invalid_file_key", f"{DAG_ID}/delta/{stem_key}_invalid.parquet",
            ],
            env_vars={**resolved_s3_env_vars, **resolved_duckdb_env_vars, **resolved_airflow_env_vars},
            do_xcom_push=False,
        ).execute(context=context)

        # ── transform_delta ──────────────────────────────────────────────────
        # Construit les URLs d'images, extrait les nutriments depuis le dict plat
        # et projette sur le schéma Silver (config/target_columns.py).
        CustomKubernetesPodOperator(
            dag=dag,
            task_id='transform_delta_pod',
            name=f'transform-delta-{pod_suffix}',
            image=IMAGE,
            arguments=[
                "--command",         "transform_delta",
                "--input_file_key",  f"{DAG_ID}/delta/{stem_key}_valid.parquet",
                "--output_file_key", f"{DAG_ID}/delta/{stem_key}_transformed.parquet",
            ],
            env_vars={**resolved_s3_env_vars},
            do_xcom_push=False,
        ).execute(context=context)

        # ── load_delta ───────────────────────────────────────────────────────
        # Upsert atomique dans MotherDuck : DELETE + INSERT dans une transaction explicite.
        # Si l'INSERT échoue, ROLLBACK annule le DELETE — la table reste dans son état d'origine.
        # Cible : off.staging.source_transformed (même table que le chargement initial).
        CustomKubernetesPodOperator(
            dag=dag,
            task_id='load_delta_pod',
            name=f'load-delta-{pod_suffix}',
            image=IMAGE,
            arguments=[
                "--command",         "load_delta",
                "--input_file_key",  f"{DAG_ID}/delta/{stem_key}_transformed.parquet",
                "--table_name",      STAGING_TABLE,
                "--schema_name",     f"{DATABASE_NAME}.{STAGING_SCHEMA}",
            ],
            env_vars={**resolved_s3_env_vars, **resolved_duckdb_env_vars},
            do_xcom_push=False,
        ).execute(context=context)

    process_mapped = process_delta_file.expand(
        stem=XComArg(save_delta_file_list)
    )

    # ── 5. Checkpoint ────────────────────────────────────────────────────────
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

    # ── 6. Fin ───────────────────────────────────────────────────────────────
    # trigger_rule ALL_DONE : s'exécute quelle que soit la branche prise
    # (process_delta_file → update_checkpoint en vert, ou skip direct en rose).
    end = EmptyOperator(
        task_id='end',
        trigger_rule='all_done',
        dag=dag,
    )

    # ── Dépendances ──────────────────────────────────────────────────────────
    start >> fetch_delta_index >> save_delta_file_list >> check_new_files
    check_new_files >> process_mapped >> update_checkpoint >> end
    check_new_files >> end
