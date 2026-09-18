"""
DAG pour le parsing des messages HL7 de Softpath
"""
# pylint: disable=invalid-name
from datetime import datetime, timedelta
from typing import List

from airflow import DAG

from lib.config import DEFAULT_PARAMS, DEFAULT_ARGS, SPARK_FAILURE_MSG, JAR, CONFIG_FILE, LOCAL_TZ
# from core.slack import Slack
from lib.groups.parsing.hl7_pdf_docling_parsing import hl7_pdf_docling_parsing
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.notify import end, start

DOC = """
# Curated Softpath HL7 DAG

ETL curated pour parser les messages HL7 softpath déposé en zone rouge

### Description
Cet ETL roule pour parser les messages HL7 et les convertir de messages .hl7 au format Delta.
Cet ETL roule 1 fois par jour.
Elle parse des données de la date précédante de la date de la run dans airflow, par exemple:
La run du 2 janvier 2020 parse les données du 1 janvier dans le lac.

### Parsing docling
La dernière étape parse avec docling les documents PDF encodés en base64 de la table OBX curated
(`curated_softpath_hl7_oru_r01_obx`) et écrit le rapport markdown, les tables extraites et un
`report.md` par document.

"""

ANONYMIZED_ZONE = "yellow"
CURATED_ZONE = "red"
ANONYMIZED_MAIN_CLASS = "bio.ferlab.ui.etl.yellow.anonymized.hl7.Main"
CURATED_MAIN_CLASS = "bio.ferlab.ui.etl.red.curated.hl7.Main"

DOCLING_INPUT_SOURCE_ID = "curated_softpath_hl7_oru_r01_obx"
DOCLING_REPORT_DELTA_DESTINATION_ID = "curated_softpath_hl7_oru_r01_obx_parsing_reports_delta"
DOCLING_TABLES_DESTINATION_ID = "curated_softpath_hl7_oru_r01_obx_parsing_tables"
DOCLING_REPORT_MD_DESTINATION_ID = "curated_softpath_hl7_oru_r01_obx_parsing_report_md"
DOCLING_DOC_BATCH_CONCURRENCY = 4
DOCLING_ENABLE_OCR = True
args = DEFAULT_ARGS.copy()
args.update({
    'provide_context': True,
    'depends_on_past': False,
    'wait_for_downstream': False})

dag = DAG(
    dag_id="curated_softpath_hl7",
    doc_md=DOC,
    start_date=datetime(1999, 12, 3, 1, tzinfo=LOCAL_TZ),
    schedule="0 1 * * *",
    params=DEFAULT_PARAMS,
    dagrun_timeout=timedelta(hours=2),
    default_args=args,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=5,
    max_active_tasks=5,
    tags=["curated"],
    on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
)

with dag:
    def get_arguments(destination: str, steps: str = "default") -> List[str]:
        """
        Generate Spark task arguments for the ETL process.
        """
        return [
            "--config", CONFIG_FILE,
            "--steps", steps,
            "--app-name", destination,
            "--destination", destination,
            "--date", "{{ ds }}"
        ]


    softpath_hl7_curated_tasks = [
        ("curated_softpath_hl7_oru_r01_pid", "small-etl"),
        ("curated_softpath_hl7_oru_r01_pv1", "small-etl"),
        ("curated_softpath_hl7_oru_r01_orc", "small-etl"),
        ("curated_softpath_hl7_oru_r01_obr", "small-etl"),
        ("curated_softpath_hl7_oru_r01_obx", "small-etl")
    ]

    softpath_hl7_curated = [SparkOperator(
        task_id=task_name,
        name=task_name.replace("_", "-"),
        arguments=get_arguments(task_name),
        zone=CURATED_ZONE,
        spark_class=CURATED_MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=SPARK_FAILURE_MSG,
        spark_config=cluster_size,
        dag=dag
    ) for task_name, cluster_size in softpath_hl7_curated_tasks]

    softpath_hl7_anonymized_tasks = [
        # ("anonymized_softpath_hl7_oru_r01_pid", "small-etl"),
        # ("anonymized_softpath_hl7_oru_r01_pv1", "small-etl"),
        # ("anonymized_softpath_hl7_oru_r01_orc", "small-etl"),
        # ("anonymized_softpath_hl7_oru_r01_obr", "small-etl"),
        ("anonymized_softpath_hl7_oru_r01_obx", "small-etl")
    ]

    softpath_hl7_anonymized = [SparkOperator(
        task_id=task_name,
        name=task_name.replace("_","-"),
        arguments=get_arguments(task_name),
        zone=ANONYMIZED_ZONE,
        spark_class=ANONYMIZED_MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=SPARK_FAILURE_MSG,
        spark_config=cluster_size,
        dag=dag
    ) for task_name, cluster_size in softpath_hl7_anonymized_tasks]

    hl7_docling_pipeline = hl7_pdf_docling_parsing(
        input_source_id=DOCLING_INPUT_SOURCE_ID,
        report_delta_destination_id=DOCLING_REPORT_DELTA_DESTINATION_ID,
        tables_destination_id=DOCLING_TABLES_DESTINATION_ID,
        report_md_destination_id=DOCLING_REPORT_MD_DESTINATION_ID,
        doc_batch_concurrency=DOCLING_DOC_BATCH_CONCURRENCY,
        enable_ocr=DOCLING_ENABLE_OCR,
    )

    start("start_curated_softpath_hl7") >> softpath_hl7_curated >> start("start_anonymized_softpath_hl7") >> softpath_hl7_anonymized >> hl7_docling_pipeline >> end("end_anonymized_softpath_hl7")
