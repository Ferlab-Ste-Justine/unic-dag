"""
DAG pour l'ingestion des data de ICCA htr se trouvant dans cathydb
"""
# pylint: disable=missing-function-docstring, duplicate-code
from datetime import datetime, timedelta
from typing import List

from airflow import DAG
from airflow.operators.empty import EmptyOperator

import pendulum

from core.config import default_params, default_args, spark_failure_msg, jar
# from core.slack import Slack
from operators.spark import SparkOperator

DOC = """
# Ingestion CathyDB DAG

ETL d'ingestion des données à haute résolution de la table ICCA htr à partir de CathyDB

### Description
Cet ETL roule pour ingérer les données à haute résolution de la table ICCA htr à partir de CathyDB depuis le 21 Mai 2015.
La date de la run dans Airflow ingère les données de la journée précédente, exemple:
la run du 2 janvier 2020 ingère les données du 1 janvier 2020 dans le lac.

"""

RAW_ZONE = "red"
RAW_MAIN_CLASS = "bio.ferlab.ui.etl.red.raw.icca.iccaHtr.Main"
ANONYMIZED_ZONE = "yellow"
ANONYMIZED_MAIN_CLASS = "bio.ferlab.ui.etl.yellow.anonymized.highresolution.Main"

args = default_args.copy()
args.update({
    'start_date': datetime(2015, 5, 21, tzinfo=pendulum.timezone("America/Montreal")),
    'provide_context': True,
    'depends_on_past': True,
    'wait_for_downstream': True})

dag = DAG(
    dag_id="ingestion_icca_htr",
    doc_md=DOC,
    start_date=datetime(2015, 5, 21, tzinfo=pendulum.timezone("America/Montreal")),
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
        task_id="start_ingestion_icca_htr",
        # on_execute_callback=Slack.notify_dag_start
    )

    raw_icca_htr = SparkOperator(
        task_id="raw_icca_icca_htr",
        name="raw-icca-icca-htr",
        arguments=arguments("raw_icca_icca_htr"),
        zone=RAW_ZONE,
        spark_class=RAW_MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="medium-etl",
        dag=dag
    )

    anonymized_icca_htr = SparkOperator(
        task_id="anonymized_icca_icca_htr",
        name="anonymized-icca-icca-htr",
        arguments=arguments("anonymized_icca_icca_htr"),
        zone=ANONYMIZED_ZONE,
        spark_class=ANONYMIZED_MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="medium-etl",
        dag=dag
    )

    publish_ingestion_icca_htr = EmptyOperator(
        task_id="publish_ingestion_icca_htr",
        # on_success_callback=Slack.notify_dag_completion
    )

    start_ingestion_icca_htr >> raw_icca_htr >> anonymized_icca_htr >> publish_ingestion_icca_htr
