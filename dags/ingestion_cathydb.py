"""
DAG pour l'ingestion des data de neonat se trouvant dans cathydb
"""
# pylint: disable=missing-function-docstring, duplicate-code, expression-not-assigned
from datetime import datetime, timedelta
from typing import List

from airflow import DAG

from lib.config import DEFAULT_PARAMS, DEFAULT_ARGS, SPARK_FAILURE_MSG, JAR
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.notify import start, end

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
INGESTION_MAIN_CLASS = "bio.ferlab.ui.etl.red.raw.cathydb.Main"

args = DEFAULT_ARGS.copy()
args.update({
    'start_date': datetime(2016, 12, 2),
    'provide_context': True})  # to use date of ingested data as input in main

dag = DAG(
    dag_id="ingestion_cathydb",
    doc_md=DOC,
    start_date=datetime(2016, 12, 2),
    end_date=datetime(2023, 8, 14),
    schedule_interval="@daily",
    params=DEFAULT_PARAMS,
    dagrun_timeout=timedelta(hours=2),
    default_args=args,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=1,  # test with 1 active dag run & 1 task can scale later
    max_active_tasks=1,
    tags=["raw"],
    on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
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

    cathydb_raw_tasks = [
        ("raw_cathydb_external_numeric", "medium-etl"),
        ("raw_cathydb_external_wave", "medium-etl"),
        ("raw_cathydb_external_patient", "medium-etl"),
        ("raw_cathydb_piicix_num", "medium-etl"),
        ("raw_cathydb_piicix_sig", "medium-etl"),
        ("raw_cathydb_piicix_alertes", "small-etl"),
    ]

    raw_spark_tasks = [SparkOperator(
        task_id=task_name,
        name=task_name.replace("_","-"), # added because I don't want to change name we have before this might impact something in postgres db not sure
        arguments=arguments(task_name),
        zone=INGESTION_ZONE,
        spark_class=INGESTION_MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=SPARK_FAILURE_MSG,
        spark_config=cluster_size,
        dag=dag
    ) for task_name, cluster_size in cathydb_raw_tasks]

    start("start_ingestion_cathydb") >> raw_spark_tasks >> end("end_ingestion_cathydb")
