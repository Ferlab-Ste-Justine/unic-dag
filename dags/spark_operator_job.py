"""
Generic Spark Operator
"""
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.utils.dates import days_ago

from spark_operators import ingestion_job

default_args = {
    "owner": "cbotek",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "cbotek@ferlab.bio"
}

JOB_TYPE = "{{ dag_run.conf.get('job_type', 'ingestion') }}"
SCHEMA = "{{ dag_run.conf.get('schema', 'pericalm') }}"
NAMESPACE = "{{ dag_run.conf.get('namespace', 'ingestion') }}"
ETL_CONFIG_FILE = "{{ dag_run.conf.get('etl_config_file', 'config/prod.conf') }}"
RUN_TYPE = "{{ dag_run.conf.get('run_type', 'default') }}"
DESTINATION = "{{ dag_run.conf.get('destination', 'raw_pericalm_addresses') }}"
POD_NAME = "{{ dag_run.conf.get('destination', 'raw_pericalm_addresses')[:40].replace('_', '-') }}"

with DAG(
        dag_id="spark_operator_job",
        schedule_interval=None,
        default_args=default_args,
        start_date=days_ago(2),
        catchup=False
) as dag:
    create_job = SparkKubernetesOperator(
        task_id="create_job",
        namespace=NAMESPACE,
        application_file=ingestion_job(NAMESPACE, POD_NAME, DESTINATION, RUN_TYPE, ETL_CONFIG_FILE),
        priority_weight=1,
        weight_rule="absolute",
        do_xcom_push=True,
        dag=dag
    )

    check_job = SparkKubernetesSensor(
        task_id="check_job",
        namespace=NAMESPACE,
        priority_weight=999,
        weight_rule="absolute",
        application_name="{{ task_instance.xcom_pull(task_ids='create_job')['metadata']['name'] }}",
        poke_interval=30,
        timeout=43200,  # 12 hours
        dag=dag,
    )

    create_job >> check_job
