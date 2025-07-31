"""
Enriched MoKa DAG
"""
# pylint: disable=missing-function-docstring, duplicate-code, expression-not-assigned
from datetime import datetime, timedelta
from typing import List

import pendulum
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from lib.config import DEFAULT_PARAMS, DEFAULT_TIMEOUT_HOURS, DEFAULT_ARGS, SPARK_FAILURE_MSG
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.notify import end, start
from lib.tasks.publish import trigger_publish_dag
from tasks import _get_version

JAR = 's3a://spark-prd/jars/unic-etl-{{ params.branch }}.jar'

DOC = """
# Enriched Moka DAG

ETL enriched pour le projet Moka. 

### Description
Cet ETL génère un rapport mensuel pour la santé mobile chez les enfants atteints d'asthme.

### Horaire
* __Date de début__ - 20 octobre 2023
* __Date de fin__ - aucune
* __Jour et heure__ - Vendredi, 7h heure de Montréal
* __Intervalle__ - Chaque 4 semaine

### Fonctionnement
Le début de l'intervalle et la fin de l'intervalle sont envoyés comme arguments à l'ETL enriched. À noter que
la fin de l'intervalle correspond au moment de génération du rapport. Donc pour le premier rapport du 20 Octobre 2023, le
début de l'intervalle est le 22 Septembre 2023. 

La date de fin de l'intervalle (date logique du DAG) est envoyée comme argument à l'ETL released. Cette date est 
utilisée comme version de la release.

Le fichier Excel doit être généré manuellement via un notebook de la zone rouge. 
"""

# Update default args
args = DEFAULT_ARGS.copy()
args.update({'trigger_rule': TriggerRule.NONE_FAILED})

dag = DAG(
    dag_id="enriched_moka",
    doc_md=DOC,
    start_date=datetime(2023, 10, 20, 7, tzinfo=pendulum.timezone("America/Montreal")),
    schedule_interval=timedelta(weeks=4),
    params=DEFAULT_PARAMS,
    dagrun_timeout=timedelta(hours=DEFAULT_TIMEOUT_HOURS),
    default_args=args,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=1,
    max_active_tasks=2,
    tags=["enriched"],
    on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
)

with dag:
    # def skip_tab() -> str:
    #     return "{% if params.skip_last_visit_survey != True %}{% else %}True{% endif %}"

    with TaskGroup(group_id="enriched") as enriched:
        ENRICHED_ZONE = "red"
        ENRICHED_MAIN_CLASS = "bio.ferlab.ui.etl.red.enriched.moka.Main"

        def enriched_arguments(destination: str, start_date: bool, end_date: bool) -> List[str]:
            arguments = [
                destination,
                "--config", "config/prod.conf",
                "--steps", "default",
                "--app-name", destination,
            ]

            if start_date:
                arguments += ["--start-date", "{{ data_interval_start }}"]

            if end_date:
                arguments += ["--end-date", "{{ data_interval_end }}"]

            return arguments


        enriched_moka_participant_index_emergency_department = SparkOperator(
            task_id="enriched_moka_participant_index_emergency_department",
            name="enriched-moka-participant-index-emergency-department",
            arguments=enriched_arguments("enriched_moka_participant_index_emergency_department", start_date=True, end_date=True),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="small-etl",
            dag=dag
        )

        enriched_moka_screening_emergency_department = SparkOperator(
            task_id="enriched_moka_screening_emergency_department",
            name="enriched-moka-screening-emergency-department",
            arguments=enriched_arguments("enriched_moka_screening_emergency_department", start_date=False, end_date=True),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="small-etl",
            dag=dag
        )

        enriched_moka_participant_index_emergency_department >> enriched_moka_screening_emergency_department

    with TaskGroup(group_id="released") as released:
        RELEASED_ZONE = "red"
        RELEASED_MAIN_CLASS = "bio.ferlab.ui.etl.green.released.Main"

        def released_arguments(destination: str, start_date: bool, end_date: bool) -> List[str]:
            arguments = [
                "--config", "config/prod.conf",
                "--steps", "default",
                "--app-name", destination,
                "--destination", destination,
                "--version", "{{ data_interval_end | ds }}"
            ]

            if start_date:
                arguments += ["--start-date", "{{ data_interval_start }}"]

            if end_date:
                arguments += ["--end-date", "{{ data_interval_end }}"]

            return arguments

        released_moka_participant_index_emergency_department = SparkOperator(
            task_id="released_moka_participant_index_emergency_department",
            name="released-moka-participant-index-emergency-department",
            arguments=released_arguments("released_moka_participant_index_emergency_department",
                                         start_date=True,
                                         end_date=True),
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="small-etl",
            dag=dag,
        )

        released_moka_screening_emergency_department = SparkOperator(
            task_id="released_moka_screening_emergency_department",
            name="released-moka-screening-emergency-department",
            arguments=released_arguments("released_moka_screening_emergency_department",
                                         start_date=False,
                                         end_date=True),
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="small-etl",
            dag=dag
        )

    with TaskGroup(group_id="published") as published:
        trigger_publish_dag_task = trigger_publish_dag(
            resource_code = "moka",
            version_to_publish = _get_version(pass_date= True, underscore= False),
            include_dictionary = True,
            skip_index = False
        )

    start() >> enriched >> released >> published >> end()
