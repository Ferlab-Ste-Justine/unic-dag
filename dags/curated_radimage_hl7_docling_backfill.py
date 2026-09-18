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
from airflow.utils.trigger_rule import TriggerRule

from lib.tasks.notify import start, end
from lib.config import DEFAULT_ARGS, LOCAL_TZ
from lib.groups.parsing.hl7_pdf_docling_parsing import hl7_pdf_docling_parsing
from lib.slack import Slack
from timetables import IntervalTimetable

INPUT_SOURCE_ID = "curated_radimage_hl7_oru_r01_obx"
REPORT_DELTA_DESTINATION_ID = "curated_radimage_hl7_oru_r01_obx_parsing_reports_delta"
TABLES_DESTINATION_ID = "curated_radimage_hl7_oru_r01_obx_parsing_tables"
REPORT_MD_DESTINATION_ID = "curated_radimage_hl7_oru_r01_obx_parsing_report_md"

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
    dagrun_timeout=timedelta(hours=10),
    is_paused_upon_creation=True,
    render_template_as_native_obj=True,
    tags=["curated", "hl7", "docling", "backfill"],
    on_failure_callback=Slack.notify_dag_failure,
)

with dag:
    start_task = start("start_curated_radimage_hl7_docling_backfill")
    end_task = end("end_curated_radimage_hl7_docling_backfill")
    hl7_docling_pipeline = hl7_pdf_docling_parsing(
        input_source_id=INPUT_SOURCE_ID,
        report_delta_destination_id=REPORT_DELTA_DESTINATION_ID,
        tables_destination_id=TABLES_DESTINATION_ID,
        report_md_destination_id=REPORT_MD_DESTINATION_ID,
        doc_batch_concurrency="{{ params.doc_batch_concurrency }}",
        enable_ocr="{{ params.enable_ocr }}",
    )
    start_task >> hl7_docling_pipeline >> end_task
