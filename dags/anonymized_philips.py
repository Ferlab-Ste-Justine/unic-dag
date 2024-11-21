"""
DAG to continue the ingestion for High-Resolution data from Philips
"""
# pylint: disable=missing-function-docstring, duplicate-code, expression-not-assigned


from datetime import datetime, timedelta

import pendulum
from airflow import DAG

from lib.config import default_params, default_args, spark_failure_msg, jar
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.notify import start, end

DOC = """
    # Anonymized Philips DAG

    ### Description
    High-Resolution Neonatal and Sip Data Anonymization ETL from Philips daily data

"""

CURATED_ZONE = "red"
ANONYMIZED_ZONE = "yellow"
TAGS = ['curated', 'anonymized']
CURATED_MAIN_CLASS = 'bio.ferlab.ui.etl.red.curated.philips.Main'
ANONYMIZED_MAIN_CLASS = "bio.ferlab.ui.etl.yellow.anonymized.philips.Main"

args = default_args.copy()
LOCAL_TZ = pendulum.timezone("America/Montreal")

args.update({
    'depends_on_past': True,
    'wait_for_downstream': False,
    'provide_context': True})

dag = DAG(
    dag_id="anonymized_philips",
    doc_md=DOC,
    start_date=datetime(2023, 8, 15, tzinfo=LOCAL_TZ),
    schedule_interval="@daily",
    params=default_params,
    dagrun_timeout=timedelta(hours=8),
    default_args=args,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=2,
    max_active_tasks=2,
    tags=TAGS,
    on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
)

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
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config=cluster_size,
        dag=dag
    )

with dag:

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

    start('start_curated_philips') >> curated_spark_tasks >> end('end_curated_philips') >> start('start_anonymized_philips') >> anonymized_spark_tasks >> end('end_ingestion_philips')
    