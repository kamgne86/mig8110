import datetime

from airflow.models import DAG
from plugins.operators.custom_kubernetes_operator import CustomKubernetesPodOperator


args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2023, 9, 12),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=60)
}


dag = DAG(
        dag_id='example_dag_k8s', 
        default_args=args,
        schedule_interval=None,
        catchup=False,
        tags=['k8s', 'custom_k8s_pod_operator']
    )

with dag:

    t1 = CustomKubernetesPodOperator(
                    dag=dag,
                    image="uqam/hello-world:1.0.0",
                    name='hello-world'
                )

    t1
