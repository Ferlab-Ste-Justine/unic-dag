"""
DAG pour l'anonymization historique des data de ICCA htr se trouvant dans cathydb
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

ETL d'anonymization des données à haute résolution historique de la table ICCA htr à partir de CathyDB

### Description
Cet ETL roule pour anonymizer les données à haute résolution de la table ICCA htr à partir de CathyDB depuis le 21 Juin 2015.
La date de la run dans Airflow ingère les données de la journée précédente, exemple:
la run du 2 janvier 2020 ingère les données du 1 janvier 2020 dans le lac.

"""

ANONYMIZED_ZONE = "yellow"
ANONYMIZED_MAIN_CLASS = "bio.ferlab.ui.etl.yellow.anonymized.highresolution.Main"

args = default_args.copy()
args.update({
    'provide_context': True,
    'depends_on_past': True,
    'wait_for_downstream': True})

dag = DAG(
    dag_id="ingestion_icca_htr",
    doc_md=DOC,
    start_date=datetime(2015, 5, 20, tzinfo=pendulum.timezone("America/Montreal")),
    end_date=datetime(2024, 1, 25, tzinfo=pendulum.timezone("America/Montreal")),
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
    start_anonymized_icca_htr = EmptyOperator(
        task_id="start_anonymized_icca_htr",
        # on_execute_callback=Slack.notify_dag_start
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

    publish_anonymized_icca_htr = EmptyOperator(
        task_id="publish_anonymized_icca_htr",
        # on_success_callback=Slack.notify_dag_completion
    )

    start_anonymized_icca_htr >> anonymized_icca_htr >> publish_anonymized_icca_htr
