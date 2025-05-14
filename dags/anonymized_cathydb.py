"""
DAG pour l'ingestion des data de neonat se trouvant dans cathydb
"""
# pylint: disable=missing-function-docstring, duplicate-code, expression-not-assigned


from datetime import datetime, timedelta
from typing import List

import pendulum
from airflow import DAG

from lib.config import DEFAULT_PARAMS, DEFAULT_ARGS, SPARK_FAILURE_MSG, JAR
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.notify import start, end

DOC = """
# Anonymized CathyDB DAG

ETL d'anonymisation' des données à haute résolution neonat et SIP à partir de CathyDB

### Description
Cet ETL roule pour anonymiser les données à haute résolution neonat et SIP à partir de CathyDB.
"""


ANONYMIZED_ZONE = "yellow"
ANONYMIZED_MAIN_CLASS = "bio.ferlab.ui.etl.yellow.anonymized.cathydb.Main"

args = DEFAULT_ARGS.copy()
LOCAL_TZ = pendulum.timezone("America/Montreal")

args.update({
    'depends_on_past': True,
    'wait_for_downstream': True,
    'provide_context': True})  # to use date of ingested data as input in main

dag = DAG(
    dag_id="anonymized_cathydb",
    doc_md=DOC,
    start_date=datetime(2017, 1, 21, tzinfo=LOCAL_TZ),
    end_date=datetime(2023, 8, 14, tzinfo=LOCAL_TZ),
    schedule_interval="@daily",
    params=DEFAULT_PARAMS,
    dagrun_timeout=timedelta(hours=2),
    default_args=args,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=1, # test with 1 active dag run & 1 task can scale later
    max_active_tasks=2,
    tags=["anonymized"],
    on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
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

    cathydb_anonymized_tasks = [
        ("anonymized_cathydb_sip_alert"        , "small-etl") ,
        ("anonymized_cathydb_neo_alert"        , "small-etl") ,
        ("anonymized_cathydb_neo_numeric_data" , "large-etl") ,
        ("anonymized_cathydb_sip_numeric_data" , "large-etl") ,
        ("anonymized_cathydb_neo_signal_data"  , "large-etl") ,
        ("anonymized_cathydb_sip_signal_data"  , "large-etl") ,
        ("anonymized_philips_neo_patient_data" , "large-etl") ,
        ("anonymized_philips_sip_patient_data" , "large-etl") ,
    ]

    anonymized_spark_tasks = [SparkOperator(
        task_id=task_name,
        name=task_name.replace("_","-"), # will do same here to make them coherent
        arguments=arguments(task_name.replace("cathydb", "philips")), # set destination to philips
        zone=ANONYMIZED_ZONE,
        spark_class=ANONYMIZED_MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=SPARK_FAILURE_MSG,
        spark_config=cluster_size,
        dag=dag
    ) for task_name, cluster_size in cathydb_anonymized_tasks]

    start("start_anonymization_cathydb") >> anonymized_spark_tasks >> end("end_ingestion_cathydb")
