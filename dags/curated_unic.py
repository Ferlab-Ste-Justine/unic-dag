"""
Curated + anonymized UNIC patient index. Publishes the patient index dataset on success.
"""
from datetime import timedelta

from airflow import DAG

from lib.config import DEFAULT_ARGS, DEFAULT_PARAMS, DEFAULT_START_DATE, CONFIG_FILE, JAR, SPARK_FAILURE_MSG
from lib.datasets import anonymized_unic_patient_index
from lib.slack import Slack
from tasks import create_tasks

TIMEOUT_HOURS = 1.5

CONFIG = {
    "steps": [
        {
            "destination_zone": "red",
            "destination_subzone": "curated",
            "main_class": "bio.ferlab.ui.etl.red.curated.Main",
            "multiple_main_methods": True,
            "datasets": [
                {"dataset_id": "curated_unic_patient_index", "cluster_type": "large", "run_type": "initial", "pass_date": False, "dependencies": []}
            ],
            "post_tests": [
                {"name": "patient_index_valid_unic_id_mapping", "destinations": ["curated_unic_patient_index"], "cluster_type": "xsmall"},
                {"name": "patient_index_valid_num_occurences", "destinations": ["curated_unic_patient_index"], "cluster_type": "xsmall"},
                {"name": "patient_index_greater_or_equal_counts", "destinations": ["curated_unic_patient_index"], "cluster_type": "xsmall"}
            ]
        },
        {
            "destination_zone": "yellow",
            "destination_subzone": "anonymized",
            "main_class": "bio.ferlab.ui.etl.yellow.anonymized.Main",
            "multiple_main_methods": False,
            "datasets": [
                {"dataset_id": "anonymized_unic_patient_index", "cluster_type": "medium", "run_type": "initial", "pass_date": False, "dependencies": []}
            ],
            "post_tests": [
                {"name": "equal_counts", "destinations": ["anonymized_unic_patient_index"], "cluster_type": "xsmall"}
            ]
        }
    ]
}

args = DEFAULT_ARGS.copy()
args["execution_timeout"] = timedelta(hours=3 / 4 * TIMEOUT_HOURS)

dag = DAG(
    dag_id="curated_unic",
    schedule="15 6 * * 1,2,5",
    params=DEFAULT_PARAMS,
    default_args=args,
    start_date=DEFAULT_START_DATE,
    concurrency=3,
    catchup=False,
    max_active_runs=1,
    tags=["curated"],
    dagrun_timeout=timedelta(hours=TIMEOUT_HOURS),
    is_paused_upon_creation=True,
    on_failure_callback=Slack.notify_dag_failure
)

with dag:
    create_tasks(dag=dag, dag_config=CONFIG, config_file=CONFIG_FILE, jar=JAR, resource="unic",
                 spark_failure_msg=SPARK_FAILURE_MSG)
    dag.get_task("anonymized.end_anonymized_unic").outlets = [anonymized_unic_patient_index]
