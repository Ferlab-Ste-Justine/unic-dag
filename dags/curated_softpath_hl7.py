"""
DAG pour le parsing des messages HL7 de Softpath
"""
# pylint: disable=duplicate-code, expression-not-assigned
from datetime import datetime, timedelta
from typing import List

import pendulum
from airflow import DAG

from lib.config import DEFAULT_PARAMS, DEFAULT_ARGS, SPARK_FAILURE_MSG, JAR
# from core.slack import Slack
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

"""

ANONYMIZED_ZONE = "yellow"
CURATED_ZONE = "red"
ANONYMIZED_MAIN_CLASS = "bio.ferlab.ui.etl.yellow.anonymized.hl7.Main"
CURATED_MAIN_CLASS = "bio.ferlab.ui.etl.red.curated.hl7.Main"
args = DEFAULT_ARGS.copy()
args.update({
    'provide_context': True,
    'depends_on_past': False,
    'wait_for_downstream': False})

dag = DAG(
    dag_id="curated_softpath_hl7",
    doc_md=DOC,
    start_date=datetime(1999, 12, 3, 1, tzinfo=pendulum.timezone("America/Montreal")),
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
            "--config", "config/prod.conf",
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

    start("start_curated_softpath_hl7") >> softpath_hl7_curated >> start("start_anonymized_softpath_hl7") >> softpath_hl7_anonymized >> end("end_anonymized_softpath_hl7")
