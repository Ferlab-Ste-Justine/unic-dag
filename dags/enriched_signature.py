"""
Enriched signature DAG
"""
from datetime import timedelta
from typing import List

import pendulum
from airflow import DAG
from airflow.models import Param
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from lib.config import DEFAULT_PARAMS, DEFAULT_TIMEOUT_HOURS, DEFAULT_ARGS, SPARK_FAILURE_MSG, MASTER_JAR, \
    CONFIG_FILE, DATE, LOCAL_TZ
from lib.operators.spark import SparkOperator
from lib.sensors.external_task import wait_for
from lib.slack import Slack
from lib.tasks.notify import end, start
from lib.tasks.publish import trigger_publish_dag
from tasks import _get_version
from timetables import IntervalTimetable

DOC = """
# Enriched Signature DAG

ETL enriched pour le projet Signature. 

### Description
Cet ETL génère un rapport mensuel sur les patients de l'étude ayant eu une ordonnance pour un test de laboratoire
depuis les quatre dernières semaines. Par défaut, les tâches liées à la table `last_visit_survey`, qui contient les
données depuis le début de l'étude, ne sont pas exécutées.

### Horaire
* __Date de début__ - 5 juin 2026
* __Date de fin__ - aucune
* __Jour et heure__ - Vendredi, 8h heure de Montréal
* __Intervalle__ - Chaque 4 semaine

### Configuration
* Paramètre `skip_last_visit_survey` : booléen indiquant si la table `last_visit_survey` doit être
skipped. Par défaut à True.

### Fonctionnement
Le début de l'intervalle et la fin de l'intervalle sont envoyés comme arguments à l'ETL enriched. Seule la tâche 
`enriched_signature_monthly_visit` a besoin de ces arguments pour filtrer les ordonnances de laboratoire. À noter que
la fin de l'intervalle correspond au moment de génération du rapport. Donc pour le premier rapport du 7 juillet 2023, le
début de l'intervalle est le 9 juin 2023. 

La date de fin de l'intervalle (date logique du DAG) est envoyée comme argument à l'ETL released. Cette date est 
utilisée comme version de la release.
"""

# Update default params
params = DEFAULT_PARAMS.copy()
params.update({"skip_last_visit_survey": Param(True, type="boolean")})
args = DEFAULT_ARGS.copy()
args.update({'trigger_rule': TriggerRule.NONE_FAILED})

dag = DAG(
    dag_id="enriched_signature",
    doc_md=DOC,
    start_date=pendulum.datetime(2026, 6, 5, 8, tz=LOCAL_TZ),
    schedule=IntervalTimetable(interval=timedelta(weeks=4)),
    params=params,
    dagrun_timeout=timedelta(hours=DEFAULT_TIMEOUT_HOURS),
    default_args=args,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,
    tags=["enriched"],
    on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
)

with dag:
    def skip_last_visit_survey() -> str:
        return "{% if params.skip_last_visit_survey != True %}{% else %}True{% endif %}"


    with TaskGroup(group_id="enriched") as enriched:
        ENRICHED_ZONE = "yellow"
        ENRICHED_MAIN_CLASS = "bio.ferlab.ui.etl.yellow.enriched.signature.Main"


        def enriched_arguments(destination: str, start_date: bool, end_date: bool) -> List[str]:
            arguments = [
                destination,
                "--config", CONFIG_FILE,
                "--steps", "default",
                "--app-name", destination,
            ]

            if start_date:
                arguments += ["--start-date", "{{ data_interval_start }}"]

            if end_date:
                arguments += ["--end-date", "{{ data_interval_end }}"]

            return arguments


        enriched_participant_index = SparkOperator(
            task_id="enriched_signature_participant_index",
            name="enriched-signature-participant-index",
            arguments=enriched_arguments("enriched_signature_participant_index", start_date=False, end_date=False),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=MASTER_JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="small-etl",
            dag=dag
        )

        enriched_last_visit_survey = SparkOperator(
            task_id="enriched_signature_last_visit_survey",
            name="enriched-signature-last-visit-survey",
            arguments=enriched_arguments("enriched_signature_last_visit_survey", start_date=True, end_date=True),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=MASTER_JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag,
            skip=skip_last_visit_survey()
        )

        enriched_monthly_visit = SparkOperator(
            task_id="enriched_signature_monthly_visit",
            name="enriched-signature-monthly-visit",
            arguments=enriched_arguments("enriched_signature_monthly_visit", start_date=True, end_date=True),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=MASTER_JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_monthly_visit_evaluation = SparkOperator(
            task_id="enriched_signature_monthly_visit_evaluation",
            name="enriched-signature-monthly-visit-evaluation",
            arguments=enriched_arguments("enriched_signature_monthly_visit_evaluation", start_date=False, end_date=True),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=MASTER_JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_participant_index >> [enriched_last_visit_survey, enriched_monthly_visit] >> enriched_monthly_visit_evaluation

    with TaskGroup(group_id="released") as released:
        RELEASED_ZONE = "green"
        RELEASED_MAIN_CLASS = "bio.ferlab.ui.etl.released.Main"

        released_last_visit_survey = SparkOperator(
            task_id="released_signature_last_visit_survey",
            name="released-signature-last-visit-survey",
            arguments=[
                "--config", CONFIG_FILE,
                "--steps", "default",
                "--app-name", "released_signature_last_visit_survey",
                "--destination", "released_signature_last_visit_survey",
                "--version", DATE
            ],
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=MASTER_JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="small-etl",
            dag=dag,
            skip=skip_last_visit_survey()
        )

        released_monthly_visit = SparkOperator(
            task_id="released_signature_monthly_visit",
            name="released-signature-monthly-visit",
            arguments=[
                "--config", CONFIG_FILE,
                "--steps", "default",
                "--app-name", "released_signature_monthly_visit",
                "--destination", "released_signature_monthly_visit",
                "--version", DATE
            ],
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=MASTER_JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="small-etl",
            dag=dag
        )

        released_monthly_visit_evaluation = SparkOperator(
            task_id="released_signature_monthly_visit_evaluation",
            name="released-signature-monthly-visit-evaluation",
            arguments=[
                "--config", CONFIG_FILE,
                "--steps", "default",
                "--app-name", "released_signature_monthly_visit_evaluation",
                "--destination", "released_signature_monthly_visit_evaluation",
                "--version", DATE
            ],
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=MASTER_JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="small-etl",
            dag=dag
        )

    with TaskGroup(group_id="published") as published:
        trigger_publish_dag_task = trigger_publish_dag(
            resource_code="signature",
            version_to_publish=_get_version(pass_date=True, underscore=False),
            include_dictionary=False,
            skip_index=True
        )

    wait_for_lab_results = wait_for("warehouse_unic", "warehouse.warehouse_lab_results",
                                    task_id="wait_for_warehouse_lab_results")
    start() >> wait_for_lab_results >> enriched >> released >> published >> end()
