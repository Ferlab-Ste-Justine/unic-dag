"""
            curated_radimage_hl7_docling_backfill
-------------------------------------------------------------------------

Backfill pipeline that parses the base64-encoded PDF documents stored in the
``observation_value_base64`` column of ``curated_radimage_hl7_oru_r01_obx`` with docling, each run
covering its own 13-week ``dte_of_message`` window and writing back the markdown report (Delta
table), the extracted tables and a ``report.md`` per document, keyed by ``dte_of_message, hl7_id``.

"""
# pylint: disable=duplicate-code, expression-not-assigned, fixme, invalid-name
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models import Param
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from kubernetes.client import models as k8s

from lib.tasks.notify import start, end
from lib.config import DEFAULT_ARGS, LOCAL_TZ, INTERVAL_START_DAY, INTERVAL_END_DAY
from lib.groups.parsing.hl7_pdf_docling_parsing import (
    DOCLING_IMAGE, extract_config, parse_and_write)
from lib.slack import Slack
from timetables import IntervalTimetable

INPUT_SOURCE_ID = "curated_radimage_hl7_oru_r01_obx"
REPORT_DELTA_DESTINATION_ID = "curated_radimage_hl7_oru_r01_obx_parsing_reports_delta"
TABLES_DESTINATION_ID = "curated_radimage_hl7_oru_r01_obx_parsing_tables"
REPORT_MD_DESTINATION_ID = "curated_radimage_hl7_oru_r01_obx_parsing_report_md"

# Overrides PARSE_EXECUTOR_CONFIG from lib.groups.parsing.hl7_pdf_docling_parsing, where the
# original and the rationale for each field live.
PARSE_POD_CPU = "8"
PARSE_POD_MEMORY = "32Gi"

RADIMAGE_EXECUTOR_CONFIG = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="base",
                    image=DOCLING_IMAGE,
                    env=[k8s.V1EnvVar(name="OMP_NUM_THREADS", value=PARSE_POD_CPU)],
                    resources=k8s.V1ResourceRequirements(
                        requests={"memory": PARSE_POD_MEMORY, "cpu": PARSE_POD_CPU},
                        limits={"memory": PARSE_POD_MEMORY, "cpu": PARSE_POD_CPU},
                    ),
                )
            ]
        )
    )
}

dag_args = DEFAULT_ARGS.copy()
dag_args.update({
    'trigger_rule': TriggerRule.NONE_FAILED,
    'on_failure_callback': Slack.notify_task_failure})

dag = DAG(
    dag_id="curated_radimage_hl7_docling_backfill",
    params={
        "doc_batch_concurrency": Param(4, type="integer",
                                       description="docling multi-document batch"),
        "enable_ocr": Param(True, type="boolean",
                            description="Run OCR for scanned PDFs (slower). Table detection is always on."),
    },
    default_args=dag_args,
    doc_md=__doc__,
    # True start of the radimage OBX history; the table holds nothing earlier.
    start_date=pendulum.datetime(2013, 10, 4, 0, tz=LOCAL_TZ),
    schedule=IntervalTimetable(interval=timedelta(weeks=13)),  # ~3 months
    catchup=True,
    max_active_runs=1,  # docling is heavy -> process backfill windows one at a time
    dagrun_timeout=timedelta(hours=14),
    is_paused_upon_creation=True,
    render_template_as_native_obj=True,
    tags=["curated", "hl7", "docling", "backfill"],
    on_failure_callback=Slack.notify_dag_failure,
)

with dag:
    start_task = start("start_curated_radimage_hl7_docling_backfill")
    end_task = end("end_curated_radimage_hl7_docling_backfill")
    # Imported tasks individually so that parse_and_write can run on a larger pod.
    with TaskGroup(group_id="hl7_pdf_docling_parsing") as hl7_docling_pipeline:
        config_dict = extract_config(
            input_source_id=INPUT_SOURCE_ID,
            report_delta_destination_id=REPORT_DELTA_DESTINATION_ID,
            tables_destination_id=TABLES_DESTINATION_ID,
            report_md_destination_id=REPORT_MD_DESTINATION_ID,
        )

        parse_and_write.override(executor_config=RADIMAGE_EXECUTOR_CONFIG)(
            config_dict=config_dict,
            input_source_id=INPUT_SOURCE_ID,
            report_delta_destination_id=REPORT_DELTA_DESTINATION_ID,
            tables_destination_id=TABLES_DESTINATION_ID,
            report_md_destination_id=REPORT_MD_DESTINATION_ID,
            interval_start=INTERVAL_START_DAY,
            interval_end=INTERVAL_END_DAY,
            doc_batch_concurrency="{{ params.doc_batch_concurrency }}",
            enable_ocr="{{ params.enable_ocr }}",
        )

    start_task >> hl7_docling_pipeline >> end_task
