import datetime
from airflow.models import DAG
from plugins.operators.duckdb_operator import DuckDBOperator


args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2023, 9, 12),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=60)
}

dag = DAG(
    dag_id='duckdb_create_schema',
    default_args=args,
    schedule_interval=None,
    catchup=False,
    tags=['duckdb', 'motherduck']
)

with dag:

    create_schema = DuckDBOperator(
        task_id='create_schema',
        sql="CREATE SCHEMA IF NOT EXISTS mig8110",
        duckdb_conn_id='duckdb_default',
    )

    create_schema
