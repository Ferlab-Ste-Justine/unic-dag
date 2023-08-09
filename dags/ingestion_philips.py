"""
DAG pour l'ingestion quotidienne des data de philips a partir de Philips
"""
# pylint: disable=duplicate-code

from datetime import datetime, timedelta
import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator


from core.config import jar, spark_failure_msg, default_params, default_args
from core.slack import Slack
from operators.spark import SparkOperator

DOC = """
# Ingestion Philips DAG

ETL d'ingestion des données neonat de Philips

### Description
Cet ETL roule quotidiennement à 3h le matin pour ingérer les données neonat enregistrées le jour précédent à partir de la BDD Philips.PatientData.
Les tables external_patient, external_patientstringattribute et external_patientdateattribute contiennent toujours des données à partir du 23 Janvier 2023.
La date de la run dans Airflow ingère les données de la journée précédente, exemple:
la run du 2 janvier 2020 ingère les données du 1 Janvier 2020 dans le lac.


### Horaire
* __Date de début__ - 29 Janvier 2023
* __Date de fin__ - aucune
* __Jour et heure__ - Chaque jour, 3h heure de Montréal
"""

NAMESPACE = "raw"
MAIN_CLASS = "bio.ferlab.ui.etl.red.raw.philips.Main"
args = default_args.copy()
args.update({
    'start_date': datetime(2023, 3, 29, 3, tzinfo=pendulum.timezone("America/Montreal")),
    'provide_context': True,  # to use date of ingested data as input in main
    'depends_on_past': True,
    'wait_for_downstream': True})

dag = DAG(
    dag_id="ingestion_philips",
    start_date=datetime(2023, 3, 29, 3, tzinfo=pendulum.timezone("America/Montreal")),
    schedule_interval=timedelta(days=1),  # everyday at 3am timezone montreal
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
        arguments=["config/prod.conf", "default", "raw_philips_external_numeric", '{{ data_interval_end | ds }}'],  # {{ds}} input date
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
        arguments=["config/prod.conf", "default", "raw_philips_external_patient", '{{ data_interval_end | ds }}'],
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
        arguments=["config/prod.conf", "default", "raw_philips_external_patientdateattribute",
                   '{{ data_interval_end | ds }}'],
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
        arguments=["config/prod.conf", "default", "raw_philips_external_patientstringattribute",
                   '{{ data_interval_end | ds }}'],
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
        arguments=["config/prod.conf", "default", "raw_philips_external_wave", '{{ data_interval_end | ds }}'],
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
        arguments=["config/prod.conf", "default", "raw_philips_external_numericvalue", '{{ data_interval_end | ds }}'],
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
        arguments=["config/prod.conf", "default", "raw_philips_external_wavesample", '{{ data_interval_end | ds }}'],
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
        arguments=["config/prod.conf", "default", "raw_philips_external_alert", '{{ data_interval_end | ds }}'],
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
