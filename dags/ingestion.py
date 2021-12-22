from yml.spark_operator_yml import ingestion_dag

# DEFAULT_ARGS = generate_default_args(owner="cbotek", on_failure_callback=task_fail_slack_alert)
default_args = {
    "owner": "cbotek",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "cbotek@ferlab.bio"
}

NAMESPACE = "ingestion"
SCHEMAS = ["eclinibase", "icca"]

for schema in SCHEMAS:
    dag_id = f"{NAMESPACE}_{schema}"
    globals()[dag_id] = ingestion_dag(dag_id, NAMESPACE, schema, default_args)
