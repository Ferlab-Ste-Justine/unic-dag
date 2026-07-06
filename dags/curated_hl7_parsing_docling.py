"""
============================================================
                curated_hl7_parsing_docling
============================================================

Dedicated pipeline that parses the base64-encoded PDF documents stored in the
``observation_value_base64`` column of curated HL7 OBX Delta tables (e.g.
``curated_radimage_hl7_oru_r01_obx``, ``curated_softpath_hl7_oru_r01_obx``) with docling,
where run parses its own ``dte_of_message`` interval window and writes back the
markdown report (Delta table) and the extracted tables, keyed by
``dte_of_message, hl7_id``.

"""
# pylint: disable=duplicate-code, expression-not-assigned, fixme, invalid-name
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models import Param
from airflow.utils.trigger_rule import TriggerRule

from lib.tasks.notify import start, end
from lib.config import DEFAULT_ARGS
from lib.groups.etl.hl7_pdf_docling_parsing import hl7_pdf_docling_parsing
from lib.slack import Slack
from timetables import IntervalTimetable

DEFAULT_DATASET_SUFFIX = "hl7_oru_r01_obx"

dag_args = DEFAULT_ARGS.copy()
dag_args.update({
    'trigger_rule': TriggerRule.NONE_FAILED,
    'on_failure_callback': Slack.notify_task_failure})

dag = DAG(
    dag_id="curated_hl7_parsing_docling",
    params={
        "resource_code": Param("softpath", type="string",
                               description="Short resource code. Ex: radimage | softpath"),
        "dataset_suffix": Param(DEFAULT_DATASET_SUFFIX, type="string",
                                description="Completes the curated source id "
                                            "curated_<resource_code>_<dataset_suffix>."),
        "doc_batch_concurrency": Param(4, type="integer",
                                       description="docling multi-document batch"),
        "enable_ocr": Param(True, type="boolean",
                            description="Run OCR for scanned PDFs (slower). Table detection is always on."),
    },
    default_args=dag_args,
    doc_md=__doc__,
    start_date=pendulum.datetime(1999, 1, 1, 0, tz="America/Montreal"),
    schedule=IntervalTimetable(interval=timedelta(weeks=13)),  # ~3 months
    catchup=True,
    max_active_runs=1,  # docling is heavy -> process backfill windows one at a time
    is_paused_upon_creation=True,
    render_template_as_native_obj=True,
    tags=["curated", "hl7", "docling"],
    on_failure_callback=Slack.notify_dag_failure,
)

with dag:
    # TODO: Turn notify=True once the DAG is stable and tested in prod env.
    start_task = start("start_curated_hl7_parsing_docling", notify=False)
    end_task = end("end_curated_hl7_parsing_docling", notify=False)
    hl7_docling_pipeline = hl7_pdf_docling_parsing(
        resource_code="{{ params.resource_code }}",
        dataset_suffix="{{ params.dataset_suffix }}",
        # Hardcoded dev s3:// destinations (polars/deltalake use s3://, not s3a://); resource_code +
        # dataset_suffix make each resource's outputs unique.
        output_text_path="s3://red-prd/curated/{{ params.resource_code }}/hl7/{{ params.dataset_suffix }}_parsed_reports",
        output_tables_tree_path="s3://red-prd/curated/{{ params.resource_code }}/hl7/{{ params.dataset_suffix }}_extracted_tables",
        doc_batch_concurrency="{{ params.doc_batch_concurrency }}",
        enable_ocr="{{ params.enable_ocr }}",
    )
    start_task >> hl7_docling_pipeline >> end_task
