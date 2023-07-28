"""
DAG pour l'ingestion quotidienne des data de neonat a partir de Philips
"""
# pylint: disable=duplicate-code
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator


from core.config import jar, spark_failure_msg, default_params, default_args
from core.slack import Slack
from operators.spark import SparkOperator

NAMESPACE = "raw"
MAIN_CLASS = "bio.ferlab.ui.etl.red.raw.neonat.MainPhilips"
args = default_args.copy()
args.update({
    'start_date': datetime(2023, 1, 23),
    'provide_context': True,  # to use date of ingested data as input in main
    'depends_on_past': True,
    'wait_for_downstream': True})

dag = DAG(
    dag_id="ingestion_neonat_philips",
    start_date=datetime(2023, 1, 23),
    schedule_interval='0 3 * * *',  # everyday at 2am EST (-5:00), 3am EDT (-4:00)
    params=default_params,
    dagrun_timeout=timedelta(hours=2),
    default_args=args,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=1,
    max_active_tasks=2,
    tags=["ingestion"]
)

with dag:
    start = EmptyOperator(
        task_id="start_ingestion_neonat_philips",
        on_execute_callback=Slack.notify_dag_start
    )

    neonat_external_numeric = SparkOperator(
        task_id="raw_neonat_external_numeric",
        name="raw-neonat-external-numeric",
        arguments=["config/prod.conf", "default", "raw_neonat_external_numeric", '{{ds}}'],  # {{ds}} input date
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="medium-etl",
        dag=dag
    )

    neonat_external_patient = SparkOperator(
        task_id="raw_neonat_external_patient",
        name="raw-neonat-external-patient",
        arguments=["config/prod.conf", "default", "raw_neonat_external_patient", '{{ds}}'],
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="xsmall-etl",
        dag=dag
    )

    neonat_external_patientdateattribute = SparkOperator(
        task_id="raw_neonat_external_patientdateattribute",
        name="raw-neonat-external-patientdateattribute",
        arguments=["config/prod.conf", "default", "raw_neonat_external_patientdateattribute", '{{ds}}'],
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="xsmall-etl",
        dag=dag
    )

    neonat_external_patientstringattribute = SparkOperator(
        task_id="raw_neonat_external_patientstringattribute",
        name="raw-neonat-external-patientstringattribute",
        arguments=["config/prod.conf", "default", "raw_neonat_external_patientstringattribute", '{{ds}}'],
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="xsmall-etl",
        dag=dag
    )

    neonat_external_wave = SparkOperator(
        task_id="raw_neonat_external_wave",
        name="raw-neonat-external-wave",
        arguments=["config/prod.conf", "default", "raw_neonat_external_wave", '{{ds}}'],
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="medium-etl",
        dag=dag
    )

    neonat_external_numericvalue = SparkOperator(
        task_id="raw_neonat_external_numericvalue",
        name="raw-neonat-external-numericvalue",
        arguments=["config/prod.conf", "default", "raw_neonat_external_numericvalue", '{{ds}}'],
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="medium-etl",
        dag=dag
    )

    neonat_external_wavesample = SparkOperator(
        task_id="raw_neonat_external_wavesample",
        name="raw-neonat-external-wavvesample",
        arguments=["config/prod.conf", "default", "raw_neonat_external_wavesample", '{{ds}}'],
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="medium-etl",
        dag=dag
    )

    neonat_external_alert = SparkOperator(
        task_id="raw_neonat_external_alert",
        name="raw-neonat-external-alert",
        arguments=["config/prod.conf", "default", "raw_neonat_external_alert", '{{ds}}'],
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="medium-etl",
        dag=dag
    )

    end = EmptyOperator(
        task_id="publish_ingestion_neonat_philips",
        on_success_callback=Slack.notify_dag_completion
    )

    start >> [neonat_external_numeric, neonat_external_wave, neonat_external_patient]
    neonat_external_patient >> [neonat_external_numericvalue, neonat_external_wavesample, neonat_external_alert,
                                neonat_external_patientdateattribute, neonat_external_patientstringattribute] >> end
