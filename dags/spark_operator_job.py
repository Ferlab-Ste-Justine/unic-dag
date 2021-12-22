from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.utils.dates import days_ago

default_args = {
    "owner": "cbotek",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "cbotek@ferlab.bio"
}

NAMESPACE = "ingestion"

with DAG(
        dag_id="spark_operator_job",
        schedule_interval=None,
        default_args=default_args,
        start_date=days_ago(2),
        max_active_runs=2,
        catchup=False
) as dag:
    create_job = SparkKubernetesOperator(
        task_id="create_job",
        namespace=NAMESPACE,
        application_file="yml/ingestion_job.yml",
        do_xcom_push=True,
        dag=dag,
    )

    check_job = SparkKubernetesSensor(
        task_id='check_job',
        namespace=NAMESPACE,
        application_name="{{ task_instance.xcom_pull(task_ids='create_job')['metadata']['name'] }}",
        dag=dag,
    )

    create_job >> check_job

