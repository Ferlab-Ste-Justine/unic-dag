"""
DAG pour le parsing des messages HL7 de Softpath
"""
# pylint: disable=duplicate-code
from datetime import datetime, timedelta

import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from core.config import default_params, default_args, spark_failure_msg, jar
from core.slack import Slack
from operators.spark import SparkOperator

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
ANONYMIZED_CLASS = "bio.ferlab.ui.etl.yellow.anonymized"
CURATED_CLASS = "bio.ferlab.ui.etl.red.curated.hl7.Main"
args = default_args.copy()
args.update({
    'provide_context': True,
    'depends_on_past': True,
    'wait_for_downstream': True})

dag = DAG(
    dag_id="curated_softpath_hl7",
    doc_md=DOC,
    start_date=datetime(2023, 9, 27, 7, tzinfo=pendulum.timezone("America/Montreal")),
    schedule_interval=timedelta(days=1),
    params=default_params,
    dagrun_timeout=timedelta(hours=2),
    default_args=args,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=1,
    max_active_tasks=2,
    tags=["curated"]
)

with dag:
    start = EmptyOperator(
        task_id="start_curated_softpath_hl7",
        on_execute_callback=Slack.notify_dag_start
    )

    curated_softpath_hl7_oru_r01 = SparkOperator(
        task_id="curated_softpath_hl7_oru_r01",
        name="curated-softpath-hl7-oru-r01",
        arguments=["config/prod.conf", "initial", "curated_softpath_hl7_oru_r01", '{{ds}}'],  # {{ds}} input date
        zone=CURATED_ZONE,
        spark_class=CURATED_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="small-etl",
        dag=dag
    )

    anonymized_softpath_hl7_oru_r01 = SparkOperator(
        task_id="anonymized_softpath_hl7_oru_r01",
        name="anonymized-softpath-hl7-oru-r01",
        arguments=["config/prod.conf", "initial", "anonymized_softpath_hl7_oru_r01"],
        zone=ANONYMIZED_ZONE,
        spark_class=ANONYMIZED_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="small-etl",
        dag=dag
    )

    end = EmptyOperator(
        task_id="publish_curated_softpath_hl7",
        on_success_callback=Slack.notify_dag_completion
    )

    start >> curated_softpath_hl7_oru_r01 >> anonymized_softpath_hl7_oru_r01 >> end
