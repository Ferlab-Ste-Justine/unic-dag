"""
DAG pour le parsing le segment OBX des messages HL7 de Softpath
"""
# pylint: disable=duplicate-code, expression-not-assigned
from datetime import datetime, timedelta
from typing import List

import pendulum
from airflow import DAG

from lib.config import DEFAULT_PARAMS, DEFAULT_ARGS, SPARK_FAILURE_MSG, JAR
from lib.operators.spark import SparkOperator
from lib.tasks.notify import start, end

DOC = """
# Anonymized Softpath HL7
Temp DAG to anonymize the OBX segment of Softpath HL7 messages
"""

ANONYMIZED_ZONE = "yellow"
ANONYMIZED_MAIN_CLASS = "bio.ferlab.ui.etl.yellow.anonymized.hl7.Main"
args = DEFAULT_ARGS.copy()
args.update({
    'provide_context': True,
    'depends_on_past': False,
    'wait_for_downstream': False})

dag = DAG(
    dag_id="anonymized_softpath_hl7_obx",
    doc_md=DOC,
    start_date=datetime(2021, 1, 1, 0, tzinfo=pendulum.timezone("America/Montreal")),
    end_date=datetime(2026, 5, 20, 0, tzinfo=pendulum.timezone("America/Montreal")),
    schedule_interval=timedelta(days=1),
    params=DEFAULT_PARAMS,
    dagrun_timeout=timedelta(hours=2),
    default_args=args,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=4,
    max_active_tasks=4,
    tags=["anonymized"],
)

with dag:
    def get_arguments(destination: str, steps: str = "default") -> List[str]:
        """
        Generate Spark task arguments for the ETL process.
        """
        return [
            "--config", "config/prod.conf",
            "--steps", steps,
            "--app-name", destination,
            "--destination", destination,
            "--date", "{{ ds }}"
        ]


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

    start("start_anonymized_softpath_hl7_obx", notify=False) >> softpath_hl7_anonymized >> end("end_anonymized_softpath_hl7_obx", notify=False)
