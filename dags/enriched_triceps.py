"""
Enriched triceps DAG
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
# Enriched Triceps DAG

ETL enriched pour le projet Triceps. 

### Description
Cet ETL génère un rapport mensuel sur les patients de l'étude ayant eu une visite en clinique génomique depuis les 
quatre dernières semaines. Ce rapport inclut les rendez-vous, l'ensemble des données des formulaires de Centro ainsi que 
les tests de laboratoire et de pathologie. La toute première exécution doit contenir toutes les données historiques, 
après cette date, le rapport devient mensuel. Une mise à jour régulière de la liste de participants sera fournie par le chercheur.

### Horaire
* __Date de début__ - 29 septembre 2023
* __Date de fin__ - aucune
* __Jour et heure__ - Vendredi, 7h heure de Montréal
* __Intervalle__ - Chaque 4 semaine

### Configuration
* Paramètre `skip_last_visit_survey` : booléen indiquant si la table `last_visit_survey` doit être
skipped. Par défaut à True.

### Fonctionnement
Le début de l'intervalle et la fin de l'intervalle sont envoyés comme arguments à l'ETL enriched. À noter que
la fin de l'intervalle correspond au moment de génération du rapport. Donc pour le premier rapport du 29 Septembre 2023, le
début de l'intervalle est le 1er Septembre 2023. 

La date de fin de l'intervalle (date logique du DAG) est envoyée comme argument à l'ETL released. Cette date est 
utilisée comme version de la release.
"""

# Update default args
args = DEFAULT_ARGS.copy()
args.update({'trigger_rule': TriggerRule.NONE_FAILED})

dag = DAG(
    dag_id="enriched_triceps",
    doc_md=DOC,
    start_date=datetime(2023, 9, 29, 7, tzinfo=pendulum.timezone("America/Montreal")),
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
        ENRICHED_ZONE = "yellow"
        ENRICHED_MAIN_CLASS = "bio.ferlab.ui.etl.yellow.enriched.triceps.Main"


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


        enriched_participant_index = SparkOperator(
            task_id="enriched_triceps_participant_index",
            name="enriched-triceps-participant-index",
            arguments=enriched_arguments("enriched_triceps_participant_index", start_date=False, end_date=False),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_appointment_information = SparkOperator(
            task_id="enriched_triceps_appointment_information",
            name="enriched-triceps-appointment-information",
            arguments=enriched_arguments("enriched_triceps_appointment_information", start_date=False, end_date=True),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag,
        )

        enriched_consultation = SparkOperator(
            task_id="enriched_triceps_consultation",
            name="enriched-triceps-consultation",
            arguments=enriched_arguments("enriched_triceps_consultation", start_date=True, end_date=True),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_laboratory_results = SparkOperator(
            task_id="enriched_triceps_laboratory_results",
            name="enriched-triceps-laboratory-results",
            arguments=enriched_arguments("enriched_triceps_laboratory_results", start_date=True, end_date=True),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_pathology_results = SparkOperator(
            task_id="enriched_triceps_pathology_results",
            name="enriched-triceps-pathology-results",
            arguments=enriched_arguments("enriched_triceps_pathology_results", start_date=True, end_date=True),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_general_information_consultation = SparkOperator(
            task_id="enriched_triceps_general_information_consultation",
            name="enriched-triceps-general-information-consultation",
            arguments=enriched_arguments("enriched_triceps_general_information_consultation", start_date=False, end_date=True),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_family_history = SparkOperator(
            task_id="enriched_triceps_family_history",
            name="enriched-triceps-family-history",
            arguments=enriched_arguments("enriched_triceps_family_history", start_date=False, end_date=True),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_family_history_consultation = SparkOperator(
            task_id="enriched_triceps_family_history_consultation",
            name="enriched-triceps-family-history-consultation",
            arguments=enriched_arguments("enriched_triceps_family_history_consultation", start_date=False, end_date=True),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_medical_history_consultation = SparkOperator(
            task_id="enriched_triceps_medical_history_consultation",
            name="enriched-triceps-medical-history-consultation",
            arguments=enriched_arguments("enriched_triceps_medical_history_consultation", start_date=False, end_date=True),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_personal_history_consultation = SparkOperator(
            task_id="enriched_triceps_personal_history_consultation",
            name="enriched-triceps-personal-history-consultation",
            arguments=enriched_arguments("enriched_triceps_personal_history_consultation", start_date=False, end_date=True),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_discussion_and_plan_consultation = SparkOperator(
            task_id="enriched_triceps_discussion_and_plan_consultation",
            name="enriched-triceps-discussion-and-plan-consultation",
            arguments=enriched_arguments("enriched_triceps_discussion_and_plan_consultation", start_date=False, end_date=True),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_personal_history_consultation_table = SparkOperator(
            task_id="enriched_triceps_personal_history_consultation_table",
            name="enriched-triceps-personal-history-consultation-table",
            arguments=enriched_arguments("enriched_triceps_personal_history_consultation_table", start_date=False, end_date=True),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_genetic_counseling_note = SparkOperator(
            task_id="enriched_triceps_genetic_counseling_note",
            name="enriched-triceps-genetic-counseling-note",
            arguments=enriched_arguments("enriched_triceps_genetic_counseling_note", start_date=False, end_date=True),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_genetic_counseling_note_discussion_and_plan = SparkOperator(
            task_id="enriched_triceps_genetic_counseling_note_discussion_and_plan",
            name="enriched-triceps-genetic-counseling-note-discussion-and-plan",
            arguments=enriched_arguments("enriched_triceps_genetic_counseling_note_discussion_and_plan", start_date=False, end_date=True),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_genetic_counseling_follow_up_note = SparkOperator(
            task_id="enriched_triceps_genetic_counseling_follow_up_note",
            name="enriched-triceps-genetic-counseling-follow-up-note",
            arguments=enriched_arguments("enriched_triceps_genetic_counseling_follow_up_note", start_date=False, end_date=True),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_presence = SparkOperator(
            task_id="enriched_triceps_presence",
            name="enriched-triceps-presence",
            arguments=enriched_arguments("enriched_triceps_presence", start_date=False, end_date=True),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_participant_index >> enriched_consultation >> enriched_appointment_information >> enriched_general_information_consultation >>  [enriched_laboratory_results, enriched_pathology_results, enriched_family_history, enriched_family_history_consultation,
                                                                                                                                                  enriched_medical_history_consultation, enriched_personal_history_consultation,
                                                                                                                                                  enriched_discussion_and_plan_consultation, enriched_personal_history_consultation_table, enriched_genetic_counseling_note,
                                                                                                                                                  enriched_genetic_counseling_note_discussion_and_plan, enriched_genetic_counseling_follow_up_note, enriched_presence]

    with TaskGroup(group_id="released") as released:
        RELEASED_ZONE = "green"
        RELEASED_MAIN_CLASS = "bio.ferlab.ui.etl.green.released.Main"


        def released_arguments(destination: str) -> List[str]:
            # {{ ds }} is the DAG run’s logical date as YYYY-MM-DD. This date is used as the released version.
            return [
                "--config", "config/prod.conf",
                "--steps", "default",
                "--app-name", destination,
                "--destination", destination,
                "--version", "{{ data_interval_end | ds }}"
            ]


        released_appointment_information = SparkOperator(
            task_id="released_triceps_appointment_information",
            name="released-triceps-appointment-information",
            arguments=released_arguments("released_triceps_appointment_information"),
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag,
        )

        released_consultation = SparkOperator(
            task_id="released_triceps_consultation",
            name="released-triceps-consultation",
            arguments=released_arguments("released_triceps_consultation"),
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        released_laboratory_results = SparkOperator(
            task_id="released_triceps_laboratory_results",
            name="released-triceps-laboratory-results",
            arguments=released_arguments("released_triceps_laboratory_results"),
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        released_pathology_results = SparkOperator(
            task_id="released_triceps_pathology_results",
            name="released-triceps-pathology-results",
            arguments=released_arguments("released_triceps_pathology_results"),
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        released_general_information_consultation = SparkOperator(
            task_id="released_triceps_general_information_consultation",
            name="released-triceps-general-information-consultation",
            arguments=released_arguments("released_triceps_general_information_consultation"),
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        released_family_history = SparkOperator(
            task_id="released_triceps_family_history",
            name="released-triceps-family-history",
            arguments=released_arguments("released_triceps_family_history"),
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        released_family_history_consultation = SparkOperator(
            task_id="released_triceps_family_history_consultation",
            name="released-triceps-family-history-consultation",
            arguments=released_arguments("released_triceps_family_history_consultation"),
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        released_medical_history_consultation = SparkOperator(
            task_id="released_triceps_medical_history_consultation",
            name="released-triceps-medical-history-consultation",
            arguments=released_arguments("released_triceps_medical_history_consultation"),
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        released_personal_history_consultation = SparkOperator(
            task_id="released_triceps_personal_history_consultation",
            name="released-triceps-personal-history-consultation",
            arguments=released_arguments("released_triceps_personal_history_consultation"),
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        released_discussion_and_plan_consultation = SparkOperator(
            task_id="released_triceps_discussion_and_plan_consultation",
            name="released-triceps-discussion-and-plan-consultation",
            arguments=released_arguments("released_triceps_discussion_and_plan_consultation"),
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        released_personal_history_consultation_table = SparkOperator(
            task_id="released_triceps_personal_history_consultation_table",
            name="released-triceps-personal-history-consultation-table",
            arguments=released_arguments("released_triceps_personal_history_consultation_table"),
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        released_genetic_counseling_note = SparkOperator(
            task_id="released_triceps_genetic_counseling_note",
            name="released-triceps-genetic-counseling-note",
            arguments=released_arguments("released_triceps_genetic_counseling_note"),
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        released_genetic_counseling_note_discussion_and_plan = SparkOperator(
            task_id="released_triceps_genetic_counseling_note_discussion_and_plan",
            name="released-triceps-genetic-counseling-note-discussion-and-plan",
            arguments=released_arguments("released_triceps_genetic_counseling_note_discussion_and_plan"),
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        released_genetic_counseling_follow_up_note = SparkOperator(
            task_id="released_triceps_genetic_counseling_follow_up_note",
            name="released-triceps-genetic-counseling-follow-up-note",
            arguments=released_arguments("released_triceps_genetic_counseling_follow_up_note"),
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

        released_presence = SparkOperator(
            task_id="released_triceps_presence",
            name="released-triceps-presence",
            arguments=released_arguments("released_triceps_presence"),
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="medium-etl",
            dag=dag
        )

    with TaskGroup(group_id="published") as published:
        trigger_publish_dag_task = trigger_publish_dag(
            resource_code="triceps",
            version_to_publish=_get_version(pass_date=True, underscore=False),
            include_dictionary=False,
            skip_index=True
        )

    start() >> enriched >> released >> published >> end()
