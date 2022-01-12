from yml.spark_operator_yml import ingestion_dag

# DEFAULT_ARGS = generate_default_args(owner="cbotek", on_failure_callback=task_fail_slack_alert)
DEFAULT_ARGS = {
    "owner": "cbotek",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "cbotek@ferlab.bio"
}

NAMESPACE = "airflow"
SCHEMAS = [
    "eclinibase",
    "icca",
    "pericalm"
]
CONFIG_FILE = "config/prod.conf"

for schema in SCHEMAS:
    dag_id = f"ingestion_{schema}".lower()
    globals()[dag_id] = ingestion_dag(dag_id, NAMESPACE, schema, CONFIG_FILE, DEFAULT_ARGS)
