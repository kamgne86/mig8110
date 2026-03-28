"""
DAG: off_weekly_delta_load
Pipeline: incremental load of Open Food Facts delta files into MotherDuck.

Steps (target):
  fetch_delta_index  → save_delta_file_list → extract_delta → filter_delta
  → validate_delta → transform_delta → load_delta → end
"""
import datetime
from airflow.models import DAG, Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
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

DELTA_INDEX_URL = "https://static.openfoodfacts.org/data/delta/index.txt"
AIRFLOW_VAR_DELTA_FILE_LIST = "delta_file_list"

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

    # Read the XCom list and persist it in an Airflow Variable for the next run
    def _save_delta_file_list(ti):
        filenames = ti.xcom_pull(task_ids='fetch-delta-index')
        Variable.set(AIRFLOW_VAR_DELTA_FILE_LIST, filenames, serialize_json=True)

    save_delta_file_list = PythonOperator(
        task_id='save_delta_file_list',
        python_callable=_save_delta_file_list,
        dag=dag,
    )

    end = EmptyOperator(task_id='end')

    delta_files = Variable.get(AIRFLOW_VAR_DELTA_FILE_LIST, default_var=[], deserialize_json=True)

    with TaskGroup(group_id='process_delta_files') as process_group:
        for filename in delta_files:
            BashOperator(
                task_id=f"log_{filename}",
                bash_command=f"echo 'Would process: {filename}'",
                dag=dag,
            )

    start >> fetch_delta_index >> save_delta_file_list >> process_group >> end

    # TODO: replace BashOperator with the real pipeline tasks
    # extract_delta >> filter_delta >> validate_delta >> transform_delta >> load_delta
