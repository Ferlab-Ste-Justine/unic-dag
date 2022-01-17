from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.utils.dates import days_ago

from yml.spark_operator_yml import ingestion_job

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
# doesn't work because the string is then trimed down the line
# DESTINATION = "{{ dag_run.conf.get('destination', 'raw_pericalm_addresses') }}"
DESTINATION = "raw_pericalm_addresses"

with DAG(
        dag_id="spark_operator_job",
        schedule_interval=None,
        default_args=default_args,
        start_date=days_ago(2),
        max_active_runs=2,
        catchup=False
) as dag:
    create_job = SparkKubernetesOperator(
        task_id="create_spark_operator_job",
        namespace=NAMESPACE,
        application_file=ingestion_job(NAMESPACE, DESTINATION, RUN_TYPE, ETL_CONFIG_FILE),
        priority_weight=1,
        weight_rule="absolute",
        do_xcom_push=True,
        dag=dag
    )

    check_job = SparkKubernetesSensor(
        task_id="check_spark_operator_job",
        namespace=NAMESPACE,
        priority_weight=999,
        weight_rule="absolute",
        application_name="{{ task_instance.xcom_pull(task_ids='create_spark_operator_job')['metadata']['name'] }}",
        poke_interval=30,
        timeout=21600,  # 6 hours
        dag=dag,
    )

    create_job >> check_job

