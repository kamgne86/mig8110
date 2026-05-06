from airflow import configuration
from airflow.models import DAG
from airflow.kubernetes.secret import Secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator


def CustomKubernetesPodOperator(name: str, image: str, dag: DAG, **kwargs) -> KubernetesPodOperator:
    
    namespace = configuration.get('kubernetes', 'NAMESPACE')
    print("namespace: ", namespace)
    in_cluster = namespace != 'default'
    config_file = None if in_cluster else '/usr/local/airflow/include/.kube/config'

    parameters = {
        'arguments': [],
        'config_file': config_file,
        'get_logs': True,
        'image': image,
        'in_cluster': in_cluster,
        'is_delete_operator_pod': True,
        'labels': {'dag-id': dag.dag_id},
        'name': name,
        'namespace': namespace,
        'task_id': name,
        #! Watchout, the kwargs overrides the default values.
        #! In other words, if you pass a parameter to the operator,
        #! it will override the value in this dictionary
        **kwargs
    }

    return KubernetesPodOperator(**parameters)
