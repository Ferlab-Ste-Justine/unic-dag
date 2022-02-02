from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

from yml.spark_operator_yml import read_json, create_spark_job, check_spark_job

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
SCHEMA = "softpath"
dagid = f"{NAMESPACE}_{SCHEMA}_static".lower()
args = DEFAULT_ARGS
main_class = "bio.ferlab.ui.etl.red.raw.softpath.Main"
config_file = CONFIG_FILE

with DAG(
        dag_id=dagid,
        schedule_interval=None,
        default_args=args,
        start_date=days_ago(2),
        concurrency=2,
        catchup=False,
        tags=["ingestion"]
) as dag:
    start = DummyOperator(
        task_id="start_operator",
        dag=dag
    )

    config = read_json(f"/opt/airflow/dags/repo/dags/config/ingestion/{SCHEMA}_config.json")

    for conf in config:

        create_job = create_spark_job(conf['dataset_id'], NAMESPACE, conf['run_type'], conf['cluster_type'], config_file, dag, main_class)
        check_job = check_spark_job(conf['dataset_id'], NAMESPACE, dag)

        start >> create_job >> check_job
