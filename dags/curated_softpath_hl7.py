"""
DAG pour le parsing des messages HL7 de Softpath
"""
# pylint: disable=duplicate-code, expression-not-assigned
from datetime import datetime, timedelta
from typing import List

import pendulum
from airflow import DAG

from lib.config import default_params, default_args, spark_failure_msg, jar
# from core.slack import Slack
from lib.operators.spark import SparkOperator
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
ANONYMIZED_MAIN_CLASS = "bio.ferlab.ui.etl.yellow.anonymized.Main"
CURATED_MAIN_CLASS = "bio.ferlab.ui.etl.red.curated.hl7.Main"
args = default_args.copy()
args.update({
    'provide_context': True,
    'depends_on_past': False,
    'wait_for_downstream': False})

dag = DAG(
    dag_id="curated_softpath_hl7",
    doc_md=DOC,
    start_date=datetime(1999, 12, 3, 7, tzinfo=pendulum.timezone("America/Montreal")),
    schedule_interval=timedelta(days=1),
    params=default_params,
    dagrun_timeout=timedelta(hours=2),
    default_args=args,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=15,
    max_active_tasks=10,
    tags=["curated"]
)

with dag:
    def get_arguments(destination: str, specific_main_method: bool, steps: str = "default") -> List[str]:
        """
        Generate Spark task arguments for the ETL process.
        """
        if specific_main_method:
            arguments = [
                destination,
                "--config", "config/prod.conf",
                "--steps", steps,
                "--app-name", destination,
                "--date", "{{ ds }}"
            ]
        else:
            arguments = [
                "--config", "config/prod.conf",
                "--steps", steps,
                "--app-name", destination,
                "--destination", destination,
                "--date", "{{ ds }}"
            ]

        return arguments


    softpath_hl7_curated_tasks = [
        ("curated_softpath_hl7_oru_r01_pid", "small-etl", False),
        ("curated_softpath_hl7_oru_r01_pv1", "small-etl", False),
        ("curated_softpath_hl7_oru_r01_orc", "small-etl", False),
        ("curated_softpath_hl7_oru_r01_obr", "small-etl", False),
        ("curated_softpath_hl7_oru_r01_obx", "small-etl", True)
    ]

    softpath_hl7_curated = [SparkOperator(
        task_id=task_name,
        name=task_name.replace("_", "-"),
        arguments=get_arguments(task_name, specific_main_method),
        zone=CURATED_ZONE,
        spark_class=CURATED_MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config=cluster_size,
        dag=dag
    ) for task_name, cluster_size, specific_main_method in softpath_hl7_curated_tasks]

    # softpath_hl7_anonymized_tasks = [
    #     ("anonymized_softpath_hl7_oru_r01_pid", "small-etl"),
    #     ("anonymized_softpath_hl7_oru_r01_pv1", "small-etl"),
    #     ("anonymized_softpath_hl7_oru_r01_orc", "small-etl"),
    #     ("anonymized_softpath_hl7_oru_r01_obr", "small-etl"),
    #     ("anonymized_softpath_hl7_oru_r01_obx", "small-etl")
    # ]
    #
    # softpath_hl7_anonymized = [SparkOperator(
    #     task_id=task_name,
    #     name=task_name.replace("_","-"),
    #     arguments=["config/prod.conf", "default", task_name, '{{ds}}'],
    #     zone=ANONYMIZED_ZONE,
    #     spark_class=ANONYMIZED_MAIN_CLASS,
    #     spark_jar=jar,
    #     spark_failure_msg=spark_failure_msg,
    #     spark_config=cluster_size,
    #     dag=dag
    # ) for task_name, cluster_size in softpath_hl7_anonymized_tasks]

    start("start_curated_softpath_hl7") >> softpath_hl7_curated >> end("end_curated_softpath_hl7")
