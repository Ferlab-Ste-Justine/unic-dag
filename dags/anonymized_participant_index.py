"""
Participant index anonymization DAG
"""
# pylint: disable=missing-function-docstring, duplicate-code, expression-not-assigned
from datetime import timedelta, datetime

from airflow import DAG
from airflow.models import Param

from lib.config import config_file, jar, spark_failure_msg, default_args
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.notify import start, end

DOC = """
# Anonymized Participant Index DAG

DAG pour l'anonymisation des index de participants des projets de recherche.

### Description
Ce DAG prend en entrée la destination à anonymiser et lance l'ETL d'anonymisation pour cette destination.

### Configuration
* Paramètre `branch` : Branche du jar à utiliser (ex. `master`)
* Paramètre `destination` : Dataset ID de la destination à anonymiser (ex. `anonymized_unic_participant_index_coprema`)
* Paramètre `run_type` : Type d'exécution de l'ETL (ex. `default`, `initial`)
"""

with DAG(
        dag_id="anonymized_participant_index",
        schedule=None,
        params={
            "branch": Param("master", type="string"),
            "destination": Param("anonymized_unic_participant_index_", type="string"),
            "run_type": Param("default", enum=["default", "initial"]),
        },
        default_args=default_args,
        doc_md=DOC,
        start_date=datetime(2021, 1, 1),
        concurrency=2,
        catchup=False,
        tags=["anonymized"],
        dagrun_timeout=timedelta(hours=1),
        is_paused_upon_creation=True,
        on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
) as dag:
    def run_type() -> str:
        return "{{ params.run_type }}"


    def destination() -> str:
        return "{{ params.destination }}"


    anonymized_task = SparkOperator(
        task_id="anonymized_participant_index",
        name="anonymized-participant-index",
        arguments=[config_file, run_type(), destination()],
        zone="yellow",
        spark_class="bio.ferlab.ui.etl.yellow.anonymized.Main",
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="xsmall-etl",
    )

    start() >> anonymized_task >> end()
