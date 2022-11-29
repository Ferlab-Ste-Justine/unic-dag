from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime


with DAG(
        dag_id='test_pod_operator_default',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
) as dag:

    test_pod_operator_default = KubernetesPodOperator(
        task_id='test_pod_operator_default',
        name='test-pod-operator-default',
        is_delete_operator_pod=True,
        config_file='~/.kube/config',
        namespace='anonymized',
        image='alpine',
        cmds=['echo', 'hello'],
        arguments=[],
    )