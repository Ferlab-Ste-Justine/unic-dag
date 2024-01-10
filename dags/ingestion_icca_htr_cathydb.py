"""
DAG pour l'ingestion des data de ICCA htr se trouvant dans cathydb
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

ETL d'ingestion des données à haute résolution de la table ICCA htr à partir de CathyDB

### Description
Cet ETL roule pour ingérer les données à haute résolution de la table ICCA htr à partir de CathyDB depuis le 2 Decembre 2016.
L'ingestion des données va s'arrêter à un certain moment en utilisant ce dag(à préciser la date exacte),
par la suite le dag philips sera exclusivement  utilisé.
La date de la run dans Airflow ingère les données de cette journée même, exemple:
la run du 1 janvier 2020 ingère les données du 1 janvier 2020 dans le lac.

"""

INGESTION_ZONE = "red"
INGESTION_MAIN_CLASS = "bio.ferlab.ui.etl.red.raw.cathydb.Main"

args = default_args.copy()
args.update({
    'start_date': datetime(2016, 12, 2),
    'provide_context': True})  # to use date of ingested data as input in main

dag = DAG(
    dag_id="ingestion_icca_htr_cathydb",
    doc_md=DOC,
    start_date=datetime(2016, 12, 2),
    end_date=datetime(2023, 8, 14),
    schedule_interval="@daily",
    params=default_params,
    dagrun_timeout=timedelta(hours=2),
    default_args=args,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=1,
    max_active_tasks=1,
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
    start_ingestion_icca_htr = EmptyOperator(
        task_id="start_ingestion_icca_htr_cathydb",
        on_execute_callback=Slack.notify_dag_start
    )

    icca_htr = SparkOperator(
        task_id="raw_cathydb_icca_htr",
        name="raw-cathydb-icca-htr",
        arguments=arguments("raw_cathydb_icca_htr"),
        zone=INGESTION_ZONE,
        spark_class=INGESTION_MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="medium-etl",
        dag=dag
    )


    publish_ingestion_icca_htr = EmptyOperator(
        task_id="publish_ingestion_icca_htr_cathydb",
        on_success_callback=Slack.notify_dag_completion
    )

    start_ingestion_icca_htr >> icca_htr >> publish_ingestion_icca_htr
