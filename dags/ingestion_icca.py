from datetime import timedelta
from typing import List

from airflow import DAG
from airflow.decorators import task_group

from lib.config import DEFAULT_PARAMS, DEFAULT_ARGS, SPARK_FAILURE_MSG, JAR, CONFIG_FILE, DEFAULT_START_DATE
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.notify import start, end
from lib.tasks.optimize import optimize
from lib.utils import sanitize_string

DOC = """
# Ingestion ICCA DAG
## Description
Ingests ICCA data from CathyDB into the raw (red) zone, then anonymizes it into the yellow zone.

## Steps
1. **raw** -- ingest each ICCA dataset into `raw_icca_*`.
2. **anonymized** -- anonymize into `anonymized_icca_*`, then optimize the anonymized tables.

## Configuration
Runs daily at 03:00.
"""

RAW_ZONE = "red"
ANONYMIZED_ZONE = "yellow"
RAW_MAIN_CLASS = "bio.ferlab.ui.etl.red.raw.icca.Main"
ANONYMIZED_MAIN_CLASS = "bio.ferlab.ui.etl.yellow.anonymized.Main"
OPTIMIZE_CLUSTER_TYPE = "medium"

# (dataset_id, cluster_type) for each step. Anonymized mirrors raw one-to-one.
RAW_DATASETS = [
    ("raw_icca_blood_gas", "small"),
    ("raw_icca_d_*", "small"),
    ("raw_icca_lits", "medium"),
    ("raw_icca_ptassessment", "medium"),
    ("raw_icca_ptbedstay", "small"),
    ("raw_icca_ptcensus", "small"),
    ("raw_icca_ptdemographic", "small"),
    ("raw_icca_ptintake", "medium"),
    ("raw_icca_ptintervention", "medium"),
    ("raw_icca_ptlabresult", "medium"),
    ("raw_icca_ptmedication", "medium"),
    ("raw_icca_ptorder", "medium"),
    ("raw_icca_ptrespiratoryorder", "medium"),
    ("raw_icca_ptsitecare", "medium"),
    ("raw_icca_pttreatment", "medium"),
]
ANONYMIZED_DATASETS = [
    ("anonymized_icca_blood_gas", "small"),
    ("anonymized_icca_d_*", "small"),
    ("anonymized_icca_lits", "medium"),
    ("anonymized_icca_ptassessment", "medium"),
    ("anonymized_icca_ptbedstay", "small"),
    ("anonymized_icca_ptcensus", "small"),
    ("anonymized_icca_ptdemographic", "small"),
    ("anonymized_icca_ptintake", "medium"),
    ("anonymized_icca_ptintervention", "medium"),
    ("anonymized_icca_ptlabresult", "medium"),
    ("anonymized_icca_ptmedication", "medium"),
    ("anonymized_icca_ptorder", "medium"),
    ("anonymized_icca_ptrespiratoryorder", "medium"),
    ("anonymized_icca_ptsitecare", "medium"),
    ("anonymized_icca_pttreatment", "medium"),
]
ANONYMIZED_OPTIMIZE = [
    "anonymized_icca_blood_gas",
    "anonymized_icca_d*",
    "anonymized_icca_lits",
    "anonymized_icca_p*",
]

TIMEOUT_HOURS = 3

args = DEFAULT_ARGS.copy()
args["execution_timeout"] = timedelta(hours=3 / 4 * TIMEOUT_HOURS)

dag = DAG(
    dag_id="ingestion_icca",
    doc_md=DOC,
    schedule_interval="0 3 * * *",
    params=DEFAULT_PARAMS,
    default_args=args,
    start_date=DEFAULT_START_DATE,
    concurrency=4,
    catchup=False,
    max_active_runs=1,
    tags=["ingestion"],
    dagrun_timeout=timedelta(hours=TIMEOUT_HOURS),
    is_paused_upon_creation=True,
    on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
)


def spark_arguments(destination: str) -> List[str]:
    """
    Generate ETL v4 Spark task arguments for an ICCA dataset.
    """
    return [
        "--config", CONFIG_FILE,
        "--steps", "default",
        "--app-name", destination,
        "--destination", destination,
    ]


def spark_task(destination: str, cluster_type: str, zone: str, main_class: str) -> SparkOperator:
    """
    Create a SparkOperator task for an ICCA dataset.
    """
    return SparkOperator(
        task_id=sanitize_string(destination, "_"),
        name=sanitize_string(destination[:40], "-"),
        arguments=spark_arguments(destination),
        zone=zone,
        spark_class=main_class,
        spark_jar=JAR,
        spark_failure_msg=SPARK_FAILURE_MSG,
        spark_config=f"{cluster_type}-etl",
        dag=dag
    )


with dag:
    @task_group(group_id="raw")
    def raw():
        raw_tasks = [spark_task(dataset_id, cluster_type, RAW_ZONE, RAW_MAIN_CLASS)
                     for dataset_id, cluster_type in RAW_DATASETS]
        start("start_raw_icca") >> raw_tasks >> end("end_raw_icca")

    @task_group(group_id="anonymized")
    def anonymized():
        anonymized_tasks = [spark_task(dataset_id, cluster_type, ANONYMIZED_ZONE, ANONYMIZED_MAIN_CLASS)
                            for dataset_id, cluster_type in ANONYMIZED_DATASETS]
        optimize_task = optimize(ANONYMIZED_OPTIMIZE, "icca", ANONYMIZED_ZONE, "anonymized",
                                 CONFIG_FILE, JAR, dag, cluster_type=OPTIMIZE_CLUSTER_TYPE)
        start("start_anonymized_icca") >> anonymized_tasks >> optimize_task >> end("end_anonymized_icca")

    raw() >> anonymized()
