"""
DAG pour l'ingestion des data de neonat se trouvant dans cathydb
"""
# pylint: disable=duplicate-code
from datetime import datetime, timedelta

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

ZONE = "red"
MAIN_CLASS = "bio.ferlab.ui.etl.red.raw.cathydb.Main"
args = default_args.copy()
args.update({
    'start_date': datetime(2016, 12, 2),
    'provide_context': True})  # to use date of ingested data as input in main

dag = DAG(
    dag_id="ingestion_cathydb",
    doc_md=DOC,
    start_date=datetime(2016, 12, 2),
    end_date=datetime(2023, 4, 7),
    schedule_interval="@daily",
    params=default_params,
    dagrun_timeout=timedelta(hours=2),
    default_args=args,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=2,
    max_active_tasks=1,
    tags=["raw"]
)

with dag:
    start = EmptyOperator(
        task_id="start_ingestion_cathydb",
        on_execute_callback=Slack.notify_dag_start
    )

    # UNCOMMENT EXTERNAL TABLES AFTER TIMESTAMP COLUMN ADDED IN CATHYDB

    cathydb_external_numeric = SparkOperator(
        task_id="raw_cathydb_external_numeric",
        name="raw-cathydb-external-numeric",
        arguments=["config/prod.conf", "default", "raw_cathydb_external_numeric", '{{ds}}'],  # {{ds}} input date
        zone=ZONE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="xlarge-etl",
        dag=dag
    )

    cathydb_external_patient = SparkOperator(
        task_id="raw_cathydb_external_patient",
        name="raw-cathydb-external-patient",
        arguments=["config/prod.conf", "default", "raw_cathydb_external_patient", '{{ds}}'],
        zone=ZONE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="small-etl",
        dag=dag
    )

    cathydb_external_wave = SparkOperator(
        task_id="raw_cathydb_external_wave",
        name="raw-cathydb-external-wave",
        arguments=["config/prod.conf", "default", "raw_cathydb_external_wave", '{{ds}}'],
        zone=ZONE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="xsmall-etl",
        dag=dag
    )

    cathydb_piicix_num = SparkOperator(
        task_id="raw_cathydb_piicix_num",
        name="raw-cathydb-piicix-num",
        arguments=["config/prod.conf", "default", "raw_cathydb_piicix_num", '{{ds}}'],
        zone=ZONE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="medium-etl",
        dag=dag
    )

    cathydb_piicix_sig = SparkOperator(
        task_id="raw_cathydb_piicix_sig",
        name="raw-cathydb-piicix-sig",
        arguments=["config/prod.conf", "default", "raw_cathydb_piicix_sig", '{{ds}}'],
        zone=ZONE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="xlarge-etl",
        dag=dag
    )

    cathydb_piicix_alertes = SparkOperator(
        task_id="raw_cathydb_piicix_alertes",
        name="raw-cathydb-piicix-alertes",
        arguments=["config/prod.conf", "default", "raw_cathydb_piicix_alertes", '{{ds}}'],
        zone=ZONE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="small-etl",
        dag=dag
    )

    end = EmptyOperator(
        task_id="publish_ingestion_cathydb",
        on_success_callback=Slack.notify_dag_completion
    )

    start >> [cathydb_external_numeric, cathydb_external_wave, cathydb_external_patient, cathydb_piicix_num,
              cathydb_piicix_sig, cathydb_piicix_alertes] >> end
