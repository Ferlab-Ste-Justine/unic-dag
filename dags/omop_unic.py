"""
UNIC OMOP. We will define a schedule in the future, for now it only runs when triggered.
"""
from datetime import timedelta

from airflow import DAG

from lib.config import DEFAULT_ARGS, DEFAULT_PARAMS, DEFAULT_START_DATE, CONFIG_FILE, JAR, SPARK_FAILURE_MSG
from lib.slack import Slack
from tasks import create_tasks

TIMEOUT_HOURS = 2

CONFIG = {
    "steps": [{
        "destination_zone": "yellow",
        "destination_subzone": "omop",
        "main_class": "bio.ferlab.ui.etl.yellow.omop.Main",
        "multiple_main_methods": True,
        "datasets": [
            {"dataset_id": "omop_location", "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
            {"dataset_id": "omop_person"  , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": ["omop_location"]},
        ]
    }]
}

args = DEFAULT_ARGS.copy()
args["execution_timeout"] = timedelta(hours=3 / 4 * TIMEOUT_HOURS)

dag = DAG(
    dag_id="omop_unic",
    schedule=None,
    params=DEFAULT_PARAMS,
    default_args=args,
    start_date=DEFAULT_START_DATE,
    concurrency=1,
    catchup=False,
    max_active_runs=1,
    tags=["omop"],
    dagrun_timeout=timedelta(hours=TIMEOUT_HOURS),
    is_paused_upon_creation=True,
    on_failure_callback=Slack.notify_dag_failure
)

with dag:
    create_tasks(dag=dag, dag_config=CONFIG, config_file=CONFIG_FILE, jar=JAR, resource="unic",
                 spark_failure_msg=SPARK_FAILURE_MSG)
