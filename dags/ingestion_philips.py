"""
DAG pour l'ingestion quotidienne des data de philips a partir de Philips
"""
# pylint: disable=missing-function-docstring, duplicate-code, expression-not-assigned

from datetime import datetime, timedelta
from typing import List

import pendulum
from airflow import DAG

from lib.config import jar, spark_failure_msg, default_params, default_args, default_timeout_hours
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.notify import start, end

DOC = """
# Ingestion Philips DAG

ETL d'ingestion des données neonat de Philips

### Description
Cet ETL roule quotidiennement à 3h le matin pour ingérer les données neonat enregistrées le jour précédent à partir de la BDD Philips.PatientData.
La date de la run dans Airflow ingère les données de la journée précédente, exemple:
la run du 15 Aout 2023 ingère les données du 14 Aout 2023  dans le lac.


### Horaire
* __Date de début__ - 15 Aout 2023 
* __Date de fin__ - aucune
* __Jour et heure__ - Chaque jour, 3h heure de Montréal
"""

ZONE = "red"
MAIN_CLASS = "bio.ferlab.ui.etl.red.raw.philips.Main"
args = default_args.copy()
args.update({
    'start_date': datetime(2023, 8, 15, 3, tzinfo=pendulum.timezone("America/Montreal")),
    'provide_context': True,  # to use date of ingested data as input in main
    'depends_on_past': True,
    'wait_for_downstream': True})

dag = DAG(
    dag_id="ingestion_philips",
    doc_md=DOC,
    start_date=datetime(2023, 8, 15, 3, tzinfo=pendulum.timezone("America/Montreal")),
    schedule_interval=timedelta(days=1),  # everyday at 3am timezone montreal
    params=default_params,
    dagrun_timeout=None,
    default_args=args,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=1,
    max_active_tasks=2,
    tags=["raw"],
    on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
)

with dag:
    def arguments(destination: str) -> List[str]:
        return [
            "--config", "config/prod.conf",
            "--steps", "default",
            "--app-name", destination,
            "--destination", destination,
            "--date", "{{ data_interval_end | ds }}"
        ]


    philips_external_numeric = SparkOperator(
        task_id="raw_philips_external_numeric",
        name="raw-philips-external-numeric",
        arguments=arguments("raw_philips_external_numeric"),
        zone=ZONE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="medium-etl",
        dag=dag
    )

    philips_external_patient = SparkOperator(
        task_id="raw_philips_external_patient",
        name="raw-philips-external-patient",
        arguments=arguments("raw_philips_external_patient"),
        zone=ZONE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="medium-etl",
        dag=dag
    )

    philips_external_patientdateattribute = SparkOperator(
        task_id="raw_philips_external_patientdateattribute",
        name="raw-philips-external-patientdateattribute",
        arguments=arguments("raw_philips_external_patientdateattribute"),
        zone=ZONE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="xsmall-etl",
        dag=dag
    )

    philips_external_patientstringattribute = SparkOperator(
        task_id="raw_philips_external_patientstringattribute",
        name="raw-philips-external-patientstringattribute",
        arguments=arguments("raw_philips_external_patientstringattribute"),
        zone=ZONE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="xsmall-etl",
        dag=dag
    )

    philips_external_wave = SparkOperator(
        task_id="raw_philips_external_wave",
        name="raw-philips-external-wave",
        arguments=arguments("raw_philips_external_wave"),
        zone=ZONE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="medium-etl",
        dag=dag
    )

    philips_external_numericvalue = SparkOperator(
        task_id="raw_philips_external_numericvalue",
        name="raw-philips-external-numericvalue",
        arguments=arguments("raw_philips_external_numericvalue"),
        zone=ZONE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="medium-etl",
        dag=dag
    )

    philips_external_wavesample = SparkOperator(
        task_id="raw_philips_external_wavesample",
        name="raw-philips-external-wavvesample",
        arguments=arguments("raw_philips_external_wavesample"),
        zone=ZONE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="medium-etl",
        dag=dag
    )

    philips_external_alert = SparkOperator(
        task_id="raw_philips_external_alert",
        name="raw-philips-external-alert",
        arguments=arguments("raw_philips_external_alert"),
        zone=ZONE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="medium-etl",
        dag=dag
    )

    start("start_ingestion_philips") >> [philips_external_numeric, philips_external_wave, philips_external_patient,
                                         philips_external_numericvalue, philips_external_wavesample,
                                         philips_external_alert,
                                         philips_external_patientdateattribute,
                                         philips_external_patientstringattribute] >> end("end_ingestion_philips")
