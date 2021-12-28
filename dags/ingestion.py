from yml.spark_operator_yml import ingestion_dag

# DEFAULT_ARGS = generate_default_args(owner="cbotek", on_failure_callback=task_fail_slack_alert)
default_args = {
    "owner": "cbotek",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "cbotek@ferlab.bio"
}

namespace = "ingestion"
schemas = ["eclinibase", "icca", "pericalm"]
config_file = "config/prod.conf"

for schema in schemas:
    dag_id = f"{namespace}_{schema}".lower()
    globals()[dag_id] = ingestion_dag(dag_id, namespace, schema, config_file, default_args)
