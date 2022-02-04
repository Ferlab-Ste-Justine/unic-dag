from airflow import DAG
from airflow.utils.dates import days_ago

from yml.spark_operator_yml import read_json, setupDag

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
SCHEMA = "softlab"
dagid = f"{NAMESPACE}_{SCHEMA}_static".lower()
args = DEFAULT_ARGS
main_class = "bio.ferlab.ui.etl.red.raw.softlab.Main"

with DAG(
        dag_id=dagid,
        schedule_interval=None,
        default_args=args,
        start_date=days_ago(2),
        concurrency=2,
        catchup=False,
        tags=["ingestion"]
) as dag:

    config = read_json(f"/opt/airflow/dags/repo/dags/config/ingestion/{SCHEMA}_config.json")

    setupDag(dag, config, NAMESPACE, CONFIG_FILE, main_class)
