"""
Ingestion and anonymized dags
"""
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

LOG_MAIN_CLASS = "bio.ferlab.ui.etl.red.raw.UpdateLog"

SCHEMAS = [

    ("anonymized", "eclinibase", "bio.ferlab.ui.etl.yellow.anonymized.Main"),
    ("anonymized", "etraceline", "bio.ferlab.ui.etl.yellow.anonymized.Main"),
    ("anonymized", "growthxp", "bio.ferlab.ui.etl.yellow.anonymized.Main"),
    ("anonymized", "laboratoire_systeme", "bio.ferlab.ui.etl.yellow.anonymized.Main"),
    ("anonymized", "medecho", "bio.ferlab.ui.etl.yellow.anonymized.Main"),
    ("anonymized", "pericalm", "bio.ferlab.ui.etl.yellow.anonymized.Main"),
    ("anonymized", "pharmacie", "bio.ferlab.ui.etl.yellow.anonymized.Main"),
    ("anonymized", "softlab", "bio.ferlab.ui.etl.yellow.anonymized.Main"),
    ("anonymized", "softmic", "bio.ferlab.ui.etl.yellow.anonymized.Main"),
    ("anonymized", "softpath", "bio.ferlab.ui.etl.yellow.anonymized.Main"),
    ("anonymized", "unic", "bio.ferlab.ui.etl.yellow.anonymized.Main"),
    ("anonymized", "staturgence", "bio.ferlab.ui.etl.yellow.anonymized.Main"),
    ("anonymized", "viewpoint5", "bio.ferlab.ui.etl.yellow.anonymized.Main"),

    ("curated", "unic", "bio.ferlab.ui.etl.red.curated.Main"),

    ("enriched", "mfm", "bio.ferlab.ui.etl.yellow.enriched.mfm.Main"),

    ("ingestion", "eclinibase", "bio.ferlab.ui.etl.red.raw.Main"),
    ("ingestion", "etraceline", "bio.ferlab.ui.etl.red.raw.Main"),
    ("ingestion", "growthxp", "bio.ferlab.ui.etl.red.raw.Main"),
    ("ingestion", "icca", "bio.ferlab.ui.etl.red.raw.Main"),
    ("ingestion", "laboratoire_systeme", "bio.ferlab.ui.etl.red.raw.Main"),
    ("ingestion", "medecho", "bio.ferlab.ui.etl.red.raw.Main"),
    ("ingestion", "pericalm", "bio.ferlab.ui.etl.red.raw.Main"),
    ("ingestion", "pharmacie", "bio.ferlab.ui.etl.red.raw.Main"),
    ("ingestion", "softlab", "bio.ferlab.ui.etl.red.raw.softlab.Main"),
    ("ingestion", "softpath", "bio.ferlab.ui.etl.red.raw.softpath.Main"),
    ("ingestion", "staturgence", "bio.ferlab.ui.etl.red.raw.Main"),
    ("ingestion", "viewpoint5", "bio.ferlab.ui.etl.red.raw.Main")
]
CONFIG_FILE = "config/prod.conf"

for namespace, schema, main_class in SCHEMAS:
    dagid = f"{namespace}_{schema}".lower()
    dag = DAG(
        dag_id=dagid,
        schedule_interval=None,
        default_args=DEFAULT_ARGS,
        start_date=days_ago(2),
        concurrency=3,
        catchup=False,
        tags=[namespace]
    )
    with dag:
        config = read_json(f"/opt/airflow/dags/repo/dags/config/{namespace}/{schema}_config.json")

        setupDag(schema, dag, config, namespace, CONFIG_FILE, main_class, LOG_MAIN_CLASS)
    globals()[dagid] = dag
