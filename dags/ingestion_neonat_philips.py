"""
DAG pour l'ingestion quotidienne des data de neonat a partir de Philips
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator

from core.slack import Slack
from operators.spark import SparkOperator

NAMESPACE = "raw"
MAIN_CLASS = "bio.ferlab.ui.etl.red.raw.neonat.MainPhilips"
JAR = 's3a://spark-prd/jars/unic-etl-{{ params.branch }}.jar'

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 8),
    'provide_context': True  # to use date of ingested data as input in main
}

dag = DAG(
    dag_id="ingestion_neonat_philips",
    schedule_interval='0 9 * * *', # everyday at 4am EST, it will be 5am after 12/3/2023 hour change
    params={
        "branch":  Param("master", type="string"),
        "version": Param("latest", type="string")
    },
    dagrun_timeout=timedelta(hours=2),
    default_args=default_args,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=2,
    max_active_tasks=3,
    tags=["ingestion"]
)

with dag:

    start = EmptyOperator(
        task_id="start_ingestion_neonat_cathydb",
        on_execute_callback=Slack.notify_dag_start
    )

    neonat_external_numeric = SparkOperator(
         task_id="raw_neonat_external_numeric",
         name="raw-neonat-external-numeric",
         arguments=["config/prod.conf", "default", "raw_neonat_external_numeric", '{{ds}}'],  # {{ds}} input date
         namespace=NAMESPACE,
         spark_class=MAIN_CLASS,
         spark_jar=JAR,
         spark_config="medium-etl",
         dag=dag
    )

    neonat_external_patient = SparkOperator(
         task_id="raw_neonat_external_patient",
         name="raw-neonat-external-patient",
         arguments=["config/prod.conf", "default", "raw_neonat_external_patient", '{{ds}}'],
         namespace=NAMESPACE,
         spark_class=MAIN_CLASS,
         spark_jar=JAR,
         spark_config="xsmall-etl",
         dag=dag
    )

    neonat_external_patientdateattribute = SparkOperator(
        task_id="raw_neonat_external_patientdateattribute",
        name="raw-neonat-external-patientdateattribute",
        arguments=["config/prod.conf", "default", "raw_neonat_external_patientdateattribute", '{{ds}}'],
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_config="xsmall-etl",
        dag=dag
    )

    neonat_external_patientstringattribute = SparkOperator(
        task_id="raw_neonat_external_patientstringattribute",
        name="raw-neonat-external-patientstringattribute",
        arguments=["config/prod.conf", "default", "raw_neonat_external_patientstringattribute", '{{ds}}'],
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_config="xsmall-etl",
        dag=dag
    )


    neonat_external_wave = SparkOperator(
         task_id="raw_neonat_external_wave",
         name="raw-neonat-external-wave",
         arguments=["config/prod.conf", "default", "raw_neonat_external_wave", '{{ds}}'],
         namespace=NAMESPACE,
         spark_class=MAIN_CLASS,
         spark_jar=JAR,
         spark_config="medium-etl",
         dag=dag
    )

    neonat_external_numericvalue = SparkOperator(
         task_id="raw_neonat_external_numericvalue",
         name="raw-neonat-external-numericvalue",
         arguments=["config/prod.conf", "default", "raw_neonat_external_numericvalue", '{{ds}}'],
         namespace=NAMESPACE,
         spark_class=MAIN_CLASS,
         spark_jar=JAR,
         spark_config="medium-etl",
         dag=dag
    )

    neonat_external_wavesample = SparkOperator(
        task_id="raw_neonat_external_wavesample",
        name="raw-neonat-external-wavvesample",
        arguments=["config/prod.conf", "default", "raw_neonat_external_wavesample", '{{ds}}'],
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_config="medium-etl",
        dag=dag
    )

    neonat_external_alert = SparkOperator(
        task_id="raw_neonat_external_alert",
        name="raw-neonat-external-alert",
        arguments=["config/prod.conf", "default", "raw_neonat_external_alert", '{{ds}}'],
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_config="medium-etl",
        dag=dag
    )

    end = EmptyOperator(
        task_id="publish_ingestion_neonat_philips",
        on_success_callback=Slack.notify_dag_completion,
        on_failure_callback=Slack.notify_task_failure
    )

    start >> [neonat_external_numeric, neonat_external_wave, neonat_external_patient]
    neonat_external_patient >> [neonat_external_numericvalue, neonat_external_wavesample, neonat_external_alert,
                                neonat_external_patientdateattribute, neonat_external_patientstringattribute] >> end
