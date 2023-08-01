"""
DAG pour l'ingestion quotidienne des data de philips a partir de Philips
"""
# pylint: disable=duplicate-code
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator


from core.config import jar, spark_failure_msg, default_params, default_args
from core.slack import Slack
from operators.spark import SparkOperator

NAMESPACE = "raw"
MAIN_CLASS = "bio.ferlab.ui.etl.red.raw.philips.Main"
args = default_args.copy()
args.update({
    'start_date': datetime(2023, 3, 29),
    'provide_context': True,  # to use date of ingested data as input in main
    'depends_on_past': True,
    'wait_for_downstream': True})

dag = DAG(
    dag_id="ingestion_philips",
    start_date=datetime(2023, 3, 29),
    schedule_interval='0 7 * * *',  # everyday at 2am EST (-5:00), 3am EDT (-4:00)
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
        task_id="start_ingestion_philips",
        on_execute_callback=Slack.notify_dag_start
    )

    philips_external_numeric = SparkOperator(
        task_id="raw_philips_external_numeric",
        name="raw-philips-external-numeric",
        arguments=["config/prod.conf", "default", "raw_philips_external_numeric", '{{ds}}'],  # {{ds}} input date
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="medium-etl",
        dag=dag
    )

    philips_external_patient = SparkOperator(
        task_id="raw_philips_external_patient",
        name="raw-philips-external-patient",
        arguments=["config/prod.conf", "default", "raw_philips_external_patient", '{{ds}}'],
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="xsmall-etl",
        dag=dag
    )

    philips_external_patientdateattribute = SparkOperator(
        task_id="raw_philips_external_patientdateattribute",
        name="raw-philips-external-patientdateattribute",
        arguments=["config/prod.conf", "default", "raw_philips_external_patientdateattribute", '{{ds}}'],
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="xsmall-etl",
        dag=dag
    )

    philips_external_patientstringattribute = SparkOperator(
        task_id="raw_philips_external_patientstringattribute",
        name="raw-philips-external-patientstringattribute",
        arguments=["config/prod.conf", "default", "raw_philips_external_patientstringattribute", '{{ds}}'],
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="xsmall-etl",
        dag=dag
    )

    philips_external_wave = SparkOperator(
        task_id="raw_philips_external_wave",
        name="raw-philips-external-wave",
        arguments=["config/prod.conf", "default", "raw_philips_external_wave", '{{ds}}'],
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="medium-etl",
        dag=dag
    )

    philips_external_numericvalue = SparkOperator(
        task_id="raw_philips_external_numericvalue",
        name="raw-philips-external-numericvalue",
        arguments=["config/prod.conf", "default", "raw_philips_external_numericvalue", '{{ds}}'],
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="medium-etl",
        dag=dag
    )

    philips_external_wavesample = SparkOperator(
        task_id="raw_philips_external_wavesample",
        name="raw-philips-external-wavvesample",
        arguments=["config/prod.conf", "default", "raw_philips_external_wavesample", '{{ds}}'],
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="medium-etl",
        dag=dag
    )

    philips_external_alert = SparkOperator(
        task_id="raw_philips_external_alert",
        name="raw-philips-external-alert",
        arguments=["config/prod.conf", "default", "raw_philips_external_alert", '{{ds}}'],
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="medium-etl",
        dag=dag
    )

    end = EmptyOperator(
        task_id="publish_ingestion_philips",
        on_success_callback=Slack.notify_dag_completion
    )

    start >> [philips_external_numeric, philips_external_wave, philips_external_patient]
    philips_external_patient >> [philips_external_numericvalue, philips_external_wavesample, philips_external_alert,
                                 philips_external_patientdateattribute, philips_external_patientstringattribute] >> end
