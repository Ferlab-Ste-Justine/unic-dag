"""
Curated Trigonix DAG
"""
# pylint: disable=duplicate-code, expression-not-assigned
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.models import Param

from lib.config import DEFAULT_PARAMS, DEFAULT_ARGS, SPARK_FAILURE_MSG, JAR, CONFIG_FILE
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.notify import start, end

DOC = """
# Curated Trigonix DAG
## Description
Builds the Trigonix scanning document index, one row per scanned document.

This DAG runs once a day. Each run processes a single landing-date folder
(`/quanum/numerisation/{date}/metadata/*.idx`); the `*` glob picks up every .idx batch
(am + pm) that landed in that folder. The .tif images themselves are NOT ingested -- they
stay in the red zone and are fetched later using the document barcode kept in clear in the index.

The run of YYYY-MM-DD+1 processes the folder dated YYYY-MM-DD (logical date `{{ ds }}`).

## Steps
1. **curated** -- `DocumentIndexETL` reads the day's .idx manifest and the `types_de_documents`
   reference, enriches each document with its name/barcode, and upserts
   `curated_trigonix_document_index` (keyed on `documentId`, idempotent). patientId is kept in clear.
2. **anonymized** -- the generic anonymized Main SHA1-pseudonymizes `patientId` into
   `anonymized_trigonix_document_index`; every other column is kept in clear on purpose.

## Configuration
* `run_type` parameter: `default` or `initial`. Defaults to `default`. Use `initial` to reset and
  rebuild the destination tables (e.g. during a backfill).
"""

CURATED_ZONE = "red"
ANONYMIZED_ZONE = "yellow"
CURATED_MAIN_CLASS = "bio.ferlab.ui.etl.red.curated.trigonix.DocumentIndexETL"
ANONYMIZED_MAIN_CLASS = "bio.ferlab.ui.etl.yellow.anonymized.Main"

CURATED_DESTINATION = "curated_trigonix_document_index"
ANONYMIZED_DESTINATION = "anonymized_trigonix_document_index"

LOCAL_TZ = pendulum.timezone("America/Montreal")

args = DEFAULT_ARGS.copy()
args.update({
    'start_date': datetime(2026, 7, 1, tzinfo=LOCAL_TZ),
    'provide_context': True})

params = DEFAULT_PARAMS.copy()
params.update({'run_type': Param('default', enum=['default', 'initial'])})

dag = DAG(
    dag_id="curated_trigonix",
    doc_md=DOC,
    start_date=datetime(2025, 2, 26, tzinfo=LOCAL_TZ),
    schedule="0 1 * * *",
    params=params,
    dagrun_timeout=timedelta(hours=2),
    default_args=args,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=3,
    max_active_tasks=3,
    tags=["curated", "anonymized"],
    on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
)


def run_type() -> str:
    return '{{ params.run_type }}'


with dag:
    curated_document_index = SparkOperator(
        task_id=CURATED_DESTINATION,
        name=CURATED_DESTINATION.replace("_", "-"),
        arguments=[
            "--config", CONFIG_FILE,
            "--steps", run_type(),
            "--app-name", CURATED_DESTINATION,
            "--date", "{{ ds }}"
        ],
        zone=CURATED_ZONE,
        spark_class=CURATED_MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=SPARK_FAILURE_MSG,
        spark_config="small-etl",
        dag=dag
    )

    anonymized_document_index = SparkOperator(
        task_id=ANONYMIZED_DESTINATION,
        name=ANONYMIZED_DESTINATION.replace("_", "-"),
        arguments=[
            "--config", CONFIG_FILE,
            "--steps", run_type(),
            "--app-name", ANONYMIZED_DESTINATION,
            "--destination", ANONYMIZED_DESTINATION
        ],
        zone=ANONYMIZED_ZONE,
        spark_class=ANONYMIZED_MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=SPARK_FAILURE_MSG,
        spark_config="small-etl",
        dag=dag
    )

    start("start_curated_trigonix") >> curated_document_index >> anonymized_document_index >> end("end_curated_trigonix")
