"""
DAG pour l'ingestion quotidienne des data de philips a partir de Philips
"""
# pylint: disable=missing-function-docstring, duplicate-code, expression-not-assigned

from datetime import datetime, timedelta
from typing import List

import pendulum
from airflow import DAG

from lib.config import JAR, SPARK_FAILURE_MSG, DEFAULT_PARAMS, DEFAULT_ARGS
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.notify import start, end

DOC = """
# Ingestion Philips DAG

ETL d'ingestion des données neonat de Philips

### Description
Cet ETL roule quotidiennement à 5h le matin pour ingérer les données neonat enregistrées le jour précédent à partir de la BDD Philips.PatientData.
La date de la run dans Airflow ingère les données de la journée précédente, exemple:
la run du 15 Aout 2023 ingère les données du 14 Aout 2023  dans le lac.


### Horaire
* __Date de début__ - 15 Aout 2023 
* __Date de fin__ - aucune
* __Jour et heure__ - Chaque jour, 5h heure de Montréal
"""

def create_spark_task(destination: str, cluster_size: str, main_class: str, zone: str, steps: str = "default") -> SparkOperator:
    """
    Create tasks for the ETL process

    Args:
        destination (str): name of the destination data to be processed
        cluster_size (str): size of cluster used
        main_class (str): Main class to use for Spark job
        zone (str): red-yellow
    Returns:
        SparkOperator
    """

    spark_args = [
        "--config", "config/prod.conf",
        "--steps", steps,
        "--app-name", destination,
        "--destination", destination,
        "--date", "{{ds}}"
    ]

    return SparkOperator(
        task_id=destination,
        name=destination,
        arguments=spark_args,
        zone=zone,
        spark_class=main_class,
        spark_jar=JAR,
        spark_failure_msg=SPARK_FAILURE_MSG,
        spark_config=cluster_size,
        dag=dag
    )

RAW_ZONE = "red"
CURATED_ZONE = "red"
ANONYMIZED_ZONE = "yellow"
TAGS = ['raw', 'curated', 'anonymized']
INGESTION_MAIN_CLASS = "bio.ferlab.ui.etl.red.raw.philips.Main"
CURATED_MAIN_CLASS = 'bio.ferlab.ui.etl.red.curated.philips.Main'
ANONYMIZED_MAIN_CLASS = "bio.ferlab.ui.etl.yellow.anonymized.philips.Main"

args = DEFAULT_ARGS.copy()
args.update({
    'start_date': datetime(2023, 8, 15, 5, tzinfo=pendulum.timezone("America/Montreal")),
    'provide_context': True,  # to use date of ingested data as input in main
    'depends_on_past': True,
    'wait_for_downstream': True})

dag = DAG(
    dag_id="ingestion_philips",
    doc_md=DOC,
    start_date=datetime(2023, 8, 15, 5, tzinfo=pendulum.timezone("America/Montreal")),
    schedule_interval=timedelta(days=1),  # every day at 2am timezone montreal
    params=DEFAULT_PARAMS,
    dagrun_timeout=timedelta(hours=12),
    default_args=args,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=1,
    max_active_tasks=2,
    tags=TAGS,
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

    philips_curated_tasks_config = [
        ('curated_philips_sip_external_patient', 'medium-etl'),
        ('curated_philips_neo_external_patient', 'medium-etl'),
    ]

    philips_anonymized_tasks_config = [
        ("anonymized_philips_sip_alert"        , "small-etl") ,
        ("anonymized_philips_neo_alert"        , "small-etl") ,
        ("anonymized_philips_neo_numeric_data" , "large-etl") ,
        ("anonymized_philips_sip_numeric_data" , "large-etl") ,
        ("anonymized_philips_neo_signal_data"  , "large-etl") ,
        ("anonymized_philips_sip_signal_data"  , "large-etl") ,
        ("anonymized_philips_neo_patient_data" , "large-etl") ,
        ("anonymized_philips_sip_patient_data" , "large-etl") ,
    ]

    curated_spark_tasks = [create_spark_task(destination, cluster_size, CURATED_MAIN_CLASS, CURATED_ZONE)
                             for destination, cluster_size in philips_curated_tasks_config]

    anonymized_spark_tasks = [create_spark_task(destination, cluster_size, ANONYMIZED_MAIN_CLASS, ANONYMIZED_ZONE)
                              for destination, cluster_size in philips_anonymized_tasks_config]

    philips_external_numeric = SparkOperator(
        task_id="raw_philips_external_numeric",
        name="raw-philips-external-numeric",
        arguments=arguments("raw_philips_external_numeric"),
        zone=RAW_ZONE,
        spark_class=INGESTION_MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=SPARK_FAILURE_MSG,
        spark_config="medium-etl",
        dag=dag
    )

    philips_external_patient = SparkOperator(
        task_id="raw_philips_external_patient",
        name="raw-philips-external-patient",
        arguments=arguments("raw_philips_external_patient"),
        zone=RAW_ZONE,
        spark_class=INGESTION_MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=SPARK_FAILURE_MSG,
        spark_config="medium-etl",
        dag=dag
    )

    philips_external_patientdateattribute = SparkOperator(
        task_id="raw_philips_external_patientdateattribute",
        name="raw-philips-external-patientdateattribute",
        arguments=arguments("raw_philips_external_patientdateattribute"),
        zone=RAW_ZONE,
        spark_class=INGESTION_MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=SPARK_FAILURE_MSG,
        spark_config="xsmall-etl",
        dag=dag
    )

    philips_external_patientstringattribute = SparkOperator(
        task_id="raw_philips_external_patientstringattribute",
        name="raw-philips-external-patientstringattribute",
        arguments=arguments("raw_philips_external_patientstringattribute"),
        zone=RAW_ZONE,
        spark_class=INGESTION_MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=SPARK_FAILURE_MSG,
        spark_config="xsmall-etl",
        dag=dag
    )

    philips_external_wave = SparkOperator(
        task_id="raw_philips_external_wave",
        name="raw-philips-external-wave",
        arguments=arguments("raw_philips_external_wave"),
        zone=RAW_ZONE,
        spark_class=INGESTION_MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=SPARK_FAILURE_MSG,
        spark_config="medium-etl",
        dag=dag
    )

    philips_external_numericvalue = SparkOperator(
        task_id="raw_philips_external_numericvalue",
        name="raw-philips-external-numericvalue",
        arguments=arguments("raw_philips_external_numericvalue"),
        zone=RAW_ZONE,
        spark_class=INGESTION_MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=SPARK_FAILURE_MSG,
        spark_config="medium-etl",
        dag=dag
    )

    philips_external_wavesample = SparkOperator(
        task_id="raw_philips_external_wavesample",
        name="raw-philips-external-wavvesample",
        arguments=arguments("raw_philips_external_wavesample"),
        zone=RAW_ZONE,
        spark_class=INGESTION_MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=SPARK_FAILURE_MSG,
        spark_config="medium-etl",
        dag=dag
    )

    philips_external_alert = SparkOperator(
        task_id="raw_philips_external_alert",
        name="raw-philips-external-alert",
        arguments=arguments("raw_philips_external_alert"),
        zone=RAW_ZONE,
        spark_class=INGESTION_MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=SPARK_FAILURE_MSG,
        spark_config="medium-etl",
        dag=dag
    )

    start("start_ingestion_philips") >> [philips_external_numeric, philips_external_wave, philips_external_patient,
                                         philips_external_numericvalue, philips_external_wavesample,
                                         philips_external_alert,
                                         philips_external_patientdateattribute,
                                         philips_external_patientstringattribute
                                        ] >> start('start_curated_philips') >> curated_spark_tasks  >> start('start_anonymized_philips') >> anonymized_spark_tasks >> end('end_anonymized_philips')
