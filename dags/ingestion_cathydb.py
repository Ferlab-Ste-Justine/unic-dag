"""
DAG pour l'ingestion des data de neonat se trouvant dans cathydb
"""
# pylint: disable=missing-function-docstring, duplicate-code
from datetime import datetime, timedelta
from typing import List

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from core.config import default_params, default_args, spark_failure_msg, jar
from core.slack import Slack
from operators.spark import SparkOperator

DOC = """
# Ingestion CathyDB DAG

ETL d'ingestion des données à haute résolution neonat et SIP à partir de CathyDB

### Description
Cet ETL roule pour ingérer les données à haute résolution neonat et SIP à partir de CathyDB depuis le 2 Decembre 2016.
L'ingestion des données neonat va s'arrêter à un certain moment en utilisant ce dag(à préciser la date exacte),
par la suite le dag philips sera exclusivement  utilisé pour neonat.
La date de la run dans Airflow ingère les données de cette journée même, exemple:
la run du 1 janvier 2020 ingère les données du 1 janvier 2020 dans le lac.

"""

INGESTION_ZONE = "red"
ANONYMIZED_ZONE = "yellow"
INGESTION_MAIN_CLASS = "bio.ferlab.ui.etl.red.raw.cathydb.Main"
ANONYMIZED_MAIN_CLASS = "bio.ferlab.ui.etl.yellow.anonymized.cathydb.Main"

args = default_args.copy()
args.update({
    'start_date': datetime(2016, 12, 2),
    'provide_context': True})  # to use date of ingested data as input in main

dag = DAG(
    dag_id="ingestion_cathydb",
    doc_md=DOC,
    start_date=datetime(2016, 12, 2),
    end_date=datetime(2023, 8, 14),
    schedule_interval="@daily",
    params=default_params,
    dagrun_timeout=timedelta(hours=2),
    default_args=args,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=20, # test with 1 active dag run & 1 task can scale later
    max_active_tasks=20,
    tags=["raw"]
)

def arguments(destination: str, steps: str = "default") -> List[str]:
    """
    Generate Spark task arguments for the ETL process
    """
    return [
        "--config", "config/prod.conf",
        "--steps", steps,
        "--app-name", destination,
        "--destination", destination,
        "--date", "{{ds}}"
    ]

with dag:
    start_ingestion_cathydb = EmptyOperator(
        task_id="start_ingestion_cathydb",
        on_execute_callback=Slack.notify_dag_start
    )

    start_anonymization_cathydb = EmptyOperator(
        task_id="start_anonymization_cathydb",
        on_execute_callback=Slack.notify_dag_start
    )

    cathydb_raw_tasks = [
        ("raw_cathydb_external_numeric", "medium-etl"),
        ("raw_cathydb_external_wave", "medium-etl"),
        ("raw_cathydb_external_patient", "medium-etl"),
        ("raw_cathydb_piicix_num", "medium-etl"),
        ("raw_cathydb_piicix_sig", "medium-etl"),
        ("raw_cathydb_piicix_alertes", "small-etl"),
    ]

    cathydb_anonymized_tasks = [
        ("anonymized_cathydb_neo_numeric_data", "large-etl"),
        ("anonymized_cathydb_sip_numeric_data", "large-etl"),
    ]

    raw_spark_tasks = [SparkOperator(
        task_id=task_name,
        name=task_name.replace("_","-"), # added because I don't want to change name we have before this might impact something in postgres db not sure
        arguments=arguments(task_name),
        zone=INGESTION_ZONE,
        spark_class=INGESTION_MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config=cluster_size,
        dag=dag
    ) for task_name, cluster_size in cathydb_raw_tasks]

    anonymized_spark_tasks = [SparkOperator(
        task_id=task_name,
        name=task_name.replace("_","-"), # will do same here to make them coherent
        arguments=arguments(task_name.replace("cathydb", "philips")), # set destination to philips
        zone=ANONYMIZED_ZONE,
        spark_class=ANONYMIZED_MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config=cluster_size,
        dag=dag
    ) for task_name, cluster_size in cathydb_anonymized_tasks]

    publish_ingestion_cathydb = EmptyOperator(
        task_id="publish_ingestion_cathydb",
        on_success_callback=Slack.notify_dag_completion
    )

    start_ingestion_cathydb >> raw_spark_tasks >> start_anonymization_cathydb >> anonymized_spark_tasks >> publish_ingestion_cathydb
