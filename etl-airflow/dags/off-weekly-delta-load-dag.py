"""
DAG: off_weekly_delta_load
Pipeline: incremental load of Open Food Facts delta files into MotherDuck.

Steps:
  fetch_delta_index    : Reads index.txt and writes the sorted file list to XCom
  save_delta_file_list : Persists the file list in an Airflow Variable
  process_delta_files  : For each file in the list:
    extract_delta      : Downloads the .json.gz, filters by country, uploads parquet to S3
    filter_delta       : Selects only the relevant columns (with fallback for renamed fields)
    validate_data      : Separates valid and invalid records
    transform_delta    : Builds image URLs, flattens nutriments, projects to Silver schema
"""
import datetime
from airflow.models import DAG, Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from plugins.operators.custom_kubernetes_operator import CustomKubernetesPodOperator

IMAGE = "mig8110/etl-images:1.0.0"
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

s3_env_vars = {
    "S3_ENDPOINT": "{{ conn.s3_conn.host }}",
    "S3_ACCESS_KEY": "{{ conn.s3_conn.login }}",
    "S3_SECRET_KEY": "{{ conn.s3_conn.password }}",
    "S3_BUCKET": "{{ conn.s3_conn.schema }}",
}

duckdb_env_vars = {
    "DUCKDB_TOKEN": "{{ conn.duckdb_default.password }}",
    "DUCKDB_DB":    "{{ conn.duckdb_default.schema }}",
}

airflow_env_vars = {
    "AIRFLOW_CTX_DAG_RUN_ID": "{{ run_id }}",
}

DELTA_INDEX_URL = "https://static.openfoodfacts.org/data/delta/index.txt"
DELTA_BASE_URL  = "https://static.openfoodfacts.org/data/delta/"

AIRFLOW_VAR_DELTA_FILE_LIST    = "delta_file_list"
AIRFLOW_VAR_LAST_PROCESSED_FILE = "delta_last_processed_file"

# Pipe syntax (target|fallback) handles fields renamed between delta file versions
FILTER_DELTA_COLUMNS = ",".join([
    "code", "brands", "product_name", "product_quantity", "product_quantity_unit",
    "quantity", "serving_quantity", "serving_size", "categories_tags", "countries_tags",
    "ecoscore_score|environmental_score_score",
    "ecoscore_grade|environmental_score_grade",
    "images", "ingredients_tags",
    "nutriscore_score", "nutriscore_grade", "nutriments",
])

with dag:

    start = EmptyOperator(task_id='start')

    # Fetch index.txt and write sorted list of filenames to XCom
    fetch_delta_index = CustomKubernetesPodOperator(
        dag=dag,
        name='fetch-delta-index',
        image=IMAGE,
        arguments=[
            "--command", "fetch_delta_index",
            "--url", DELTA_INDEX_URL,
        ],
        do_xcom_push=True,
    )

    # Read the XCom list and persist it in an Airflow Variable for subsequent runs
    def _save_delta_file_list(ti):
        filenames = ti.xcom_pull(task_ids='fetch-delta-index')
        Variable.set(AIRFLOW_VAR_DELTA_FILE_LIST, filenames, serialize_json=True)

    save_delta_file_list = PythonOperator(
        task_id='save_delta_file_list',
        python_callable=_save_delta_file_list,
        dag=dag,
    )

    end = EmptyOperator(task_id='end')

    # Compute which files to process at DAG parse time:
    # - First run (last_processed_file=None): process all files in the list
    # - Subsequent runs: process only files strictly after the last processed one
    all_delta_files   = Variable.get(AIRFLOW_VAR_DELTA_FILE_LIST, default_var=[], deserialize_json=True)
    last_processed    = Variable.get(AIRFLOW_VAR_LAST_PROCESSED_FILE, default_var=None)
    files_to_process  = [f for f in all_delta_files if f > last_processed] if last_processed else all_delta_files

    # Save the last processed file after all delta files have been processed
    def _update_checkpoint():
        if files_to_process:
            Variable.set(AIRFLOW_VAR_LAST_PROCESSED_FILE, files_to_process[-1])

    update_checkpoint = PythonOperator(
        task_id='update_checkpoint',
        python_callable=_update_checkpoint,
        dag=dag,
    )

    with TaskGroup(group_id='process_delta_files') as process_group:
        for filename in files_to_process:
            stem = filename.replace(".json.gz", "")

            raw_key         = f"{DAG_ID}/delta/{stem}.parquet"
            filtered_key    = f"{DAG_ID}/delta/{stem}_filtered.parquet"
            valid_key       = f"{DAG_ID}/delta/{stem}_valid.parquet"
            invalid_key     = f"{DAG_ID}/delta/{stem}_invalid.parquet"
            transformed_key = f"{DAG_ID}/delta/{stem}_transformed.parquet"

            # Download .json.gz, filter by country, serialize to parquet
            extract = CustomKubernetesPodOperator(
                dag=dag,
                name=f"extract-delta-{stem}",
                task_id=f"extract_delta_{stem}",
                image=IMAGE,
                env_vars={**s3_env_vars},
                arguments=[
                    "--command", "extract_delta",
                    "--filename", filename,
                    "--base_url", DELTA_BASE_URL,
                    "--output_file_key", raw_key,
                ],
            )

            # Select only the relevant columns (with fallback for renamed delta fields)
            filter_delta = CustomKubernetesPodOperator(
                dag=dag,
                name=f"filter-delta-{stem}",
                task_id=f"filter_delta_{stem}",
                image=IMAGE,
                env_vars={**s3_env_vars},
                arguments=[
                    "--command", "filter_delta",
                    "--input_file_key",  raw_key,
                    "--output_file_key", filtered_key,
                    "--columns", FILTER_DELTA_COLUMNS,
                ],
            )

            # Separate valid and invalid records
            validate_data = CustomKubernetesPodOperator(
                dag=dag,
                name=f"validate-data-{stem}",
                task_id=f"validate_data_{stem}",
                image=IMAGE,
                env_vars={**s3_env_vars, **duckdb_env_vars, **airflow_env_vars},
                arguments=[
                    "--command", "validate_data",
                    "--input_file_key",   filtered_key,
                    "--output_file_key",  valid_key,
                    "--invalid_file_key", invalid_key,
                ],
            )

            # Build image URLs, flatten nutriments, project to Silver schema
            transform_delta = CustomKubernetesPodOperator(
                dag=dag,
                name=f"transform-delta-{stem}",
                task_id=f"transform_delta_{stem}",
                image=IMAGE,
                env_vars={**s3_env_vars},
                arguments=[
                    "--command", "transform_delta",
                    "--input_file_key",  valid_key,
                    "--output_file_key", transformed_key,
                ],
            )

            extract >> filter_delta >> validate_data >> transform_delta

    start >> fetch_delta_index >> save_delta_file_list >> process_group >> update_checkpoint >> end
