"""
Enriched ValidationIndexPatient DAG
"""
# pylint: disable=missing-function-docstring, duplicate-code, expression-not-assigned
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule

from lib.config import default_params, default_timeout_hours, default_args, spark_failure_msg
from lib.config import jar
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.notify import end, start

DOC = """
# Enriched ValidationIndexPatient DAG

### Description
ETL Enriched pour le projet de validation de l'index patient visant à fournir aux archivistes la liste des numéros de 
dossier dupliqués. Cette liste devrait être livrée de façon mensuelle.
"""

# Update default args
args = default_args.copy()
args.update({'trigger_rule': TriggerRule.NONE_FAILED})

dag = DAG(
    dag_id="enriched_validationindexpatient",
    doc_md=DOC,
    start_date=datetime(2024, 10, 9, 13, tzinfo=pendulum.timezone("America/Montreal")),
    schedule_interval=timedelta(weeks=4),
    params=default_params,
    dagrun_timeout=timedelta(hours=default_timeout_hours),
    default_args=args,
    is_paused_upon_creation=False,
    catchup=True,
    max_active_runs=1,
    max_active_tasks=2,
    tags=["enriched"],
    on_failure_callback=Slack.notify_task_failure  # Should send notification to Slack when DAG exceeds timeout
)


with dag:

    def enriched():
        enriched_zone = "red"
        enriched_main_class = "bio.ferlab.ui.etl.red.enriched.validationindexpatient.Main"

        return SparkOperator(
            task_id="enriched_validationindexpatient_patients_dupliques",
            name="enriched-validationindexpatient-patients-dupliques",
            arguments=["config/prod.conf", "default", "enriched_validationindexpatient_patients_dupliques"],
            zone=enriched_zone,
            spark_class=enriched_main_class,
            spark_jar=jar,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

    enriched_validationindexpatient_patients_dupliques = enriched()

    start("start_enriched_validationindexpatient") >> enriched_validationindexpatient_patients_dupliques >> end("end_enriched_validationindexpatient")
