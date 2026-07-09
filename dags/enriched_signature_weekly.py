"""
Enriched Signature Weekly DAG. Waits for the warehouse pathology table before running.
"""
from datetime import timedelta

import pendulum
from airflow import DAG

from lib.config import DEFAULT_ARGS, DEFAULT_PARAMS, LOCAL_TZ, CONFIG_FILE, JAR, SPARK_FAILURE_MSG
from lib.sensors.external_task import wait_for
from lib.slack import Slack
from tasks import create_tasks

TIMEOUT_HOURS = 4

CONFIG = {
    "steps": [
        {
            "destination_zone": "yellow",
            "destination_subzone": "enriched",
            "main_class": "bio.ferlab.ui.etl.yellow.enriched.signature.Main",
            "multiple_main_methods": True,
            "datasets": [
                {"dataset_id": "enriched_signature_participant_index"      , "cluster_type": "small", "run_type": "default", "pass_date": False, "dependencies": []},
                {"dataset_id": "enriched_signature_weekly_appointment"     , "cluster_type": "small", "run_type": "default", "pass_date": True, "dependencies": ["enriched_signature_participant_index"]},
                {"dataset_id": "enriched_signature_weekly_pathology_report", "cluster_type": "small", "run_type": "default", "pass_date": True, "dependencies": ["enriched_signature_participant_index"]},
            ]
        },
        {
            "destination_zone": "green",
            "destination_subzone": "released",
            "main_class": "bio.ferlab.ui.etl.released.Main",
            "multiple_main_methods": False,
            "datasets": [
                {"dataset_id": "released_signature_weekly_appointment"     , "cluster_type": "small", "run_type": "default", "pass_date": True, "dependencies": []},
                {"dataset_id": "released_signature_weekly_pathology_report", "cluster_type": "small", "run_type": "default", "pass_date": True, "dependencies": []},
            ]
        },
        {
            "destination_subzone": "published",
            "resource_code": "signature",
            "pass_date": True,
            "include_dictionary": False
        }
    ]
}

args = DEFAULT_ARGS.copy()
args["execution_timeout"] = timedelta(hours=3 / 4 * TIMEOUT_HOURS)

dag = DAG(
    dag_id="enriched_signature_weekly",
    schedule="0 8 * * 5",
    params=DEFAULT_PARAMS,
    default_args=args,
    start_date=pendulum.datetime(2025, 11, 14, tz=LOCAL_TZ),
    concurrency=2,
    catchup=True,
    max_active_runs=1,
    tags=["enriched"],
    dagrun_timeout=timedelta(hours=TIMEOUT_HOURS),
    is_paused_upon_creation=True,
    on_failure_callback=Slack.notify_dag_failure
)

with dag:
    create_tasks(dag=dag, dag_config=CONFIG, config_file=CONFIG_FILE, jar=JAR, resource="signature_weekly",
                 spark_failure_msg=SPARK_FAILURE_MSG)
    wait_for("warehouse_unic", "warehouse.warehouse_pathology", task_id="wait_for_warehouse_pathology") \
        >> dag.get_task("enriched.start_enriched_signature_weekly")
