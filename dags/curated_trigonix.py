"""
Curated Trigonix DAG
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param

from lib.config import DEFAULT_PARAMS, DEFAULT_ARGS, SPARK_FAILURE_MSG, JAR, CONFIG_FILE, DEFAULT_START_DATE
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.notify import start, end

DOC = """
# Curated Trigonix DAG
## Description
Builds the Trigonix scanning document index, one row per scanned document. Runs daily on the
day's `.idx` manifest (`/quanum/numerisation/{{ ds }}/metadata/*.idx`); no `.idx` for the day
means nothing is written.

## Steps
1. **curated** (red zone) -- `DocumentIndexETL` reads the `.idx` index, joins the
   `types_de_documents` reference to add the document-type name and barcode, and upserts
   `curated_trigonix_document_index` (key `documentId`).
2. **anonymized** (yellow zone) -- `Main` applies `TrigonixMappings` into `anonymized_trigonix_document_index`.

## Configuration
* `run_type` parameter: `default` or `initial`. Defaults to `default`. `initial` rebuilds the
  destination tables.
"""

CURATED_ZONE = "red"
ANONYMIZED_ZONE = "yellow"
CURATED_MAIN_CLASS = "bio.ferlab.ui.etl.red.curated.trigonix.DocumentIndexETL"
ANONYMIZED_MAIN_CLASS = "bio.ferlab.ui.etl.yellow.anonymized.Main"

CURATED_DESTINATION = "curated_trigonix_document_index"
ANONYMIZED_DESTINATION = "anonymized_trigonix_document_index"

args = DEFAULT_ARGS.copy()
args.update({
    'provide_context': True})

params = DEFAULT_PARAMS.copy()
params.update({'run_type': Param('default', enum=['default', 'initial'])})

dag = DAG(
    dag_id="curated_trigonix",
    doc_md=DOC,
    start_date=datetime(2026, 7, 1, tzinfo=DEFAULT_START_DATE.tzinfo),
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
