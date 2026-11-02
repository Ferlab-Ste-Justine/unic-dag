"""
DAG de test des ETLs d'identifiants UNIC
"""
from datetime import timedelta
from typing import List

from airflow import DAG
from airflow.decorators import task
from airflow.models import DagRun, Param
from airflow.utils.trigger_rule import TriggerRule

from lib.config import DEFAULT_PARAMS, DEFAULT_ARGS, DEFAULT_TIMEOUT_HOURS, SPARK_FAILURE_MSG, JAR, CONFIG_FILE, \
    QA_TEST_MAIN_CLASS, QA_TEST_CLUSTER_TYPE, QA_TEST_SPARK_FAILURE_MSG, QA_TEST_RETRIES
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.notify import end, start

DOC = """
# Curated UNIC Identifiers DAG

Test DAG for the identifiers ETLs.

### Tasks
1. `curated_unic_identifiers_common_names` and `curated_unic_identifiers_street_names`: the replacement names, rebuilt
   from every version of eclinibase. Skipped when `skip_names` is true.
2. `curated_unic_identifiers_pathology`: the identifiers of the softpath HL7 messages of the run date. The pid messages
   whose MRN is not in the patient index go to `curated_unic_identifiers_pathology_pending`.
3. `curated_unic_identifiers_pathology_pending_rows`: fails when the run left pending messages on the run date.

### Triggering
* `date` parameter (required): the run date, `YYYY-MM-DD`, e.g. `2026-10-28` processes the HL7 messages of 2026-10-28.
* `skip_names` parameter: `true` to skip the common names and street names ETLs. Defaults to `true`. The names tables
  must exist before the pathology ETL runs.
"""

CURATED_ZONE = "red"
CURATED_MAIN_CLASS = "bio.ferlab.ui.etl.red.curated.Main"

NAMES_DESTINATIONS = ["curated_unic_identifiers_common_names", "curated_unic_identifiers_street_names"]
PATHOLOGY_DESTINATION = "curated_unic_identifiers_pathology"
PATHOLOGY_PENDING_DESTINATION = "curated_unic_identifiers_pathology_pending"
PATHOLOGY_PENDING_ROWS_TASK_ID = f"{PATHOLOGY_DESTINATION}_pending_rows"
RUN_DATE = "{{ params.date }}"

params = DEFAULT_PARAMS.copy()
params.update({'date': Param(type="string", format="date", description="Run date, YYYY-MM-DD. Required"),
               'skip_names': Param(True, type="boolean",
                                   description="Skip the common names and street names ETLs. Default: True")})

dag = DAG(
    dag_id="curated_unic_identifiers",
    doc_md=DOC,
    schedule=None,
    params=params,
    dagrun_timeout=timedelta(hours=DEFAULT_TIMEOUT_HOURS),
    default_args=DEFAULT_ARGS,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=1,  # The pathology ETL merges into its stored rows
    tags=["curated"],
    on_failure_callback=Slack.notify_dag_failure
)

with dag:
    def get_arguments(destination: str, pass_date: bool = False) -> List[str]:
        """
        Generate Spark task arguments for the ETL process.
        """
        args = [
            destination,
            "--config", CONFIG_FILE,
            "--steps", "default",
            "--app-name", destination
        ]
        return args + ["--date", RUN_DATE] if pass_date else args

    @task
    def get_skip_names(ti=None) -> bool:
        """
        Get the skip_names parameter of the run.
        """
        dag_run: DagRun = ti.dag_run
        return dag_run.conf['skip_names']

    get_skip_names_task = get_skip_names()

    names = [SparkOperator(
        task_id=destination,
        name=destination.replace("_", "-")[:40],
        arguments=get_arguments(destination),
        zone=CURATED_ZONE,
        spark_class=CURATED_MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=SPARK_FAILURE_MSG,
        spark_config="medium-etl",
        skip=get_skip_names_task,
        dag=dag
    ) for destination in NAMES_DESTINATIONS]

    pathology = SparkOperator(
        task_id=PATHOLOGY_DESTINATION,
        name=PATHOLOGY_DESTINATION.replace("_", "-")[:40],
        arguments=get_arguments(PATHOLOGY_DESTINATION, pass_date=True),
        zone=CURATED_ZONE,
        spark_class=CURATED_MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=SPARK_FAILURE_MSG,
        spark_config="large-etl",
        trigger_rule=TriggerRule.NONE_FAILED,  # Runs when the names tasks are skipped
        dag=dag
    )

    pathology_pending_rows = SparkOperator(
        task_id=PATHOLOGY_PENDING_ROWS_TASK_ID,
        name=PATHOLOGY_PENDING_ROWS_TASK_ID.replace("_", "-")[:40],
        arguments=[
            "pending_rows",
            "--config", CONFIG_FILE,
            "--steps", "default",
            "--app-name", PATHOLOGY_PENDING_ROWS_TASK_ID,
            "--destination", PATHOLOGY_PENDING_DESTINATION,
            "--date", RUN_DATE
        ],
        zone=CURATED_ZONE,
        spark_class=QA_TEST_MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=QA_TEST_SPARK_FAILURE_MSG,
        spark_config=f"{QA_TEST_CLUSTER_TYPE}-etl",
        retries=QA_TEST_RETRIES,
        dag=dag
    )

    start("start_curated_unic_identifiers") >> get_skip_names_task >> names >> pathology >> pathology_pending_rows >> end("end_curated_unic_identifiers")
