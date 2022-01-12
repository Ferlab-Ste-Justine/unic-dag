from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.utils.dates import days_ago
from yml.spark_operator_yml import read_json, ingestion_job

now = datetime.now()
dt_string = now.strftime("%d%m%Y-%H%M%S")
# DEFAULT_ARGS = generate_default_args(owner="cbotek", on_failure_callback=task_fail_slack_alert)
DEFAULT_ARGS = {
    "owner": "cbotek",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "cbotek@ferlab.bio"
}
NAMESPACE = "ingestion"
CONFIG_FILE = "config/prod.conf"
dagid = f"{NAMESPACE}_pericalm".lower()
args = DEFAULT_ARGS
namespace = NAMESPACE
schema = "pericalm"
config_file = CONFIG_FILE

with DAG(
        dag_id=dagid,
        schedule_interval=None,
        default_args=args,
        start_date=days_ago(2),
        concurrency=2,
        catchup=False
) as dag:
    start = DummyOperator(
        task_id="start_operator",
        dag=dag
    )
    config = read_json(f"/Users/christophebotek/airflow/dags/config/{namespace}/{schema}_config.json")
    for conf in config:
        destination = conf['dataset_id']
        create_job = SparkKubernetesOperator(
            task_id=f"create_{destination}",
            namespace=namespace,
            application_file=ingestion_job(destination, conf['run_type'], config_file),
            priority_weight=1,
            weight_rule="absolute",
            do_xcom_push=True,
            dag=dag
        )
        check_job = SparkKubernetesSensor(
            task_id=f'check_{destination}',
            namespace=namespace,
            priority_weight=999,
            weight_rule="absolute",
            application_name=f"{{{{ task_instance.xcom_pull(task_ids='create_{destination}')['metadata']['name'] }}}}",
            poke_interval=30,
            timeout=21600,  # 6 hours
            dag=dag,
        )
        start >> create_job >> check_job