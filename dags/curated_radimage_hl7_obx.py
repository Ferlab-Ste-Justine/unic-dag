"""
DAG pour le parsing le segment OBX des messages HL7 de Radimage
"""
# pylint: disable=duplicate-code, expression-not-assigned
from datetime import datetime, timedelta

import pendulum
from airflow import DAG

from lib.config import default_params, default_args, spark_failure_msg, jar
# from core.slack import Slack
from lib.operators.spark import SparkOperator
from lib.tasks.notify import start, end

DOC = """
# Curated Radimage HL7 DAG

ETL temporaire curated pour le parsing le segment OBX des messages HL7 de Radimage

### Description
Cet ETL roule pour re-ingérer l'historique du segment OBX et de les convertir en format delta.
Une fois que l'ingestion de l'historique est complété, cet ETL ne va plus être utilisé.
Elle parse des données de la date précédante de la date de la run dans airflow, par exemple:
La run du 2 janvier 2020 parse les données du 1 janvier dans le lac.

"""

CURATED_ZONE = "red"
CURATED_MAIN_CLASS = "bio.ferlab.ui.etl.red.curated.hl7.Main"
args = default_args.copy()
args.update({
    'provide_context': True})

dag = DAG(
    dag_id="curated_radimage_hl7_obx",
    doc_md=DOC,
    start_date=datetime(2013, 10, 5, 7, tzinfo=pendulum.timezone("America/Montreal")),
    end_date=datetime(2024, 6, 5, tzinfo=pendulum.timezone("America/Montreal")),
    schedule_interval=timedelta(days=1),
    params=default_params,
    dagrun_timeout=timedelta(hours=2),
    default_args=args,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=5,
    max_active_tasks=5,
    tags=["curated"]
)

with dag:
    arguments = [
        "curated_radimage_hl7_oru_r01_obx",
        "--config", "config/prod.conf",
        "--steps", "default",
        "--app-name", "curated_radimage_hl7_oru_r01_obx",
        "--date", "{{ ds }}"
    ]

    radimage_hl7_curated = SparkOperator(
        task_id="curated_radimage_hl7_oru_r01_obx",
        name="curated-radimage-hl7-oru-r01-obx",
        arguments=arguments,
        zone=CURATED_ZONE,
        spark_class=CURATED_MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="small-etl",
        dag=dag
    )

    start("start_curated_radimage_hl7_obx", notify=False) >> radimage_hl7_curated >> end("end_curated_radimage_hl7_obx", notify=False)
