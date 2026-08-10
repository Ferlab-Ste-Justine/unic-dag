"""
UNIC warehouse reference tables. Waits for the warehouse tables they read before running.
"""
from datetime import timedelta

from airflow import DAG

from lib.config import DEFAULT_ARGS, DEFAULT_PARAMS, DEFAULT_START_DATE, CONFIG_FILE, JAR, SPARK_FAILURE_MSG
from lib.sensors.external_task import wait_for
from lib.slack import Slack
from tasks import create_tasks

TIMEOUT_HOURS = 3
# Leaves room for warehouse_unic to finish: it is dataset-triggered by curated_unic, so its
# completion time drifts. Kept below execution_timeout so the sensor fails before the task does.
WAIT_TIMEOUT_SECONDS = 2 * 60 * 60

CONFIG = {
    "steps": [{
        "destination_zone": "yellow",
        "destination_subzone": "warehouse",
        "main_class": "bio.ferlab.ui.etl.yellow.warehouse.Main",
        "multiple_main_methods": False,
        "datasets": [
            {"dataset_id": "warehouse_reference_laboratory_information_system"                 , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
            {"dataset_id": "warehouse_reference_laboratory_information_system_aggregations"    , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
            {"dataset_id": "warehouse_reference_laboratory_information_system_associated_tests", "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []}
        ]
    }]
}

args = DEFAULT_ARGS.copy()
args["execution_timeout"] = timedelta(hours=3 / 4 * TIMEOUT_HOURS)

dag = DAG(
    dag_id="warehouse_unic_reference",
    schedule="0 9 * * 1,2,5",
    params=DEFAULT_PARAMS,
    default_args=args,
    start_date=DEFAULT_START_DATE,
    max_active_tasks=2,
    catchup=False,
    max_active_runs=1,
    tags=["warehouse"],
    dagrun_timeout=timedelta(hours=TIMEOUT_HOURS),
    is_paused_upon_creation=True,
    on_failure_callback=Slack.notify_dag_failure
)

with dag:
    create_tasks(dag=dag, dag_config=CONFIG, config_file=CONFIG_FILE, jar=JAR, resource="unic_reference",
                 spark_failure_msg=SPARK_FAILURE_MSG)
    wait_for("warehouse_unic", "warehouse.warehouse_lab_results", "warehouse.warehouse_microbiology",
             "warehouse.warehouse_pathology", "warehouse.warehouse_sociodemographics",
             task_id="wait_for_warehouse", timeout=WAIT_TIMEOUT_SECONDS) \
        >> dag.get_task("warehouse.start_warehouse_unic_reference")
