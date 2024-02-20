"""
Enriched triceps DAG
"""
# pylint: disable=missing-function-docstring, duplicate-code
from datetime import datetime, timedelta
from typing import List

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from core.config import default_params, default_timeout_hours, default_args, spark_failure_msg
from core.slack import Slack
from operators.spark import SparkOperator

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
* __Jour et heure__ - Vendredi, 8h heure de Montréal
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
args = default_args.copy()
args.update({'trigger_rule': TriggerRule.NONE_FAILED})

dag = DAG(
    dag_id="enriched_triceps",
    doc_md=DOC,
    start_date=datetime(2023, 9, 29, 8, tzinfo=pendulum.timezone("America/Montreal")),
    schedule_interval=timedelta(weeks=4),
    params=default_params,
    dagrun_timeout=timedelta(hours=default_timeout_hours),
    default_args=args,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=1,
    max_active_tasks=2,
    tags=["enriched"]
)

with dag:
    # def skip_tab() -> str:
    #     return "{% if params.skip_last_visit_survey != True %}{% else %}True{% endif %}"

    start = EmptyOperator(
        task_id="start",
        on_execute_callback=Slack.notify_dag_start
    )

    with TaskGroup(group_id="enriched") as enriched:
        ENRICHED_ZONE = "yellow"
        ENRICHED_MAIN_CLASS = "bio.ferlab.ui.etl.yellow.enriched.triceps.Main"

        def enriched_arguments(destination: str) -> List[str]:
            # !!! Do not set to initial, otherwise the participant index will be re-generated !!!
            return ["config/prod.conf", "default", destination, "{{ data_interval_start }}", "{{ data_interval_end }}"]


        enriched_participant_index = SparkOperator(
            task_id="enriched_triceps_participant_index",
            name="enriched-triceps-participant-index",
            arguments=enriched_arguments("enriched_triceps_participant_index"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_appointment_information = SparkOperator(
            task_id="enriched_triceps_appointment_information",
            name="enriched-triceps-appointment-information",
            arguments=enriched_arguments("enriched_triceps_appointment_information"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag,
        )

        enriched_consultation = SparkOperator(
            task_id="enriched_triceps_consultation",
            name="enriched-triceps-consultation",
            arguments=enriched_arguments("enriched_triceps_consultation"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_laboratory_results = SparkOperator(
            task_id="enriched_triceps_laboratory_results",
            name="enriched-triceps-laboratory-results",
            arguments=enriched_arguments("enriched_triceps_laboratory_results"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_pathology_results = SparkOperator(
            task_id="enriched_triceps_pathology_results",
            name="enriched-triceps-pathology-results",
            arguments=enriched_arguments("enriched_triceps_pathology_results"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_general_information_consultation = SparkOperator(
            task_id="enriched_triceps_general_information_consultation",
            name="enriched-triceps-general-information-consultation",
            arguments=enriched_arguments("enriched_triceps_general_information_consultation"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_family_history = SparkOperator(
            task_id="enriched_triceps_family_history",
            name="enriched-triceps-family-history",
            arguments=enriched_arguments("enriched_triceps_family_history"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_family_history_consultation = SparkOperator(
            task_id="enriched_triceps_family_history_consultation",
            name="enriched-triceps-family-history-consultation",
            arguments=enriched_arguments("enriched_triceps_family_history_consultation"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_medical_history_consultation = SparkOperator(
            task_id="enriched_triceps_medical_history_consultation",
            name="enriched-triceps-medical-history-consultation",
            arguments=enriched_arguments("enriched_triceps_medical_history_consultation"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_personal_history_consultation = SparkOperator(
            task_id="enriched_triceps_personal_history_consultation",
            name="enriched-triceps-personal-history-consultation",
            arguments=enriched_arguments("enriched_triceps_personal_history_consultation"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_discussion_and_plan_consultation = SparkOperator(
            task_id="enriched_triceps_discussion_and_plan_consultation",
            name="enriched-triceps-discussion-and-plan-consultation",
            arguments=enriched_arguments("enriched_triceps_discussion_and_plan_consultation"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_personal_history_consultation_table = SparkOperator(
            task_id="enriched_triceps_personal_history_consultation_table",
            name="enriched-triceps-personal-history-consultation-table",
            arguments=enriched_arguments("enriched_triceps_personal_history_consultation_table"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_genetic_counseling_note = SparkOperator(
            task_id="enriched_triceps_genetic_counseling_note",
            name="enriched-triceps-genetic-counseling-note",
            arguments=enriched_arguments("enriched_triceps_genetic_counseling_note"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_genetic_counseling_note_discussion_and_plan = SparkOperator(
            task_id="enriched_triceps_genetic_counseling_note_discussion_and_plan",
            name="enriched-triceps-genetic-counseling-note-discussion-and-plan",
            arguments=enriched_arguments("enriched_triceps_genetic_counseling_note_discussion_and_plan"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_genetic_counseling_follow_up_note = SparkOperator(
            task_id="enriched_triceps_genetic_counseling_follow_up_note",
            name="enriched-triceps-genetic-counseling-follow-up-note",
            arguments=enriched_arguments("enriched_triceps_genetic_counseling_follow_up_note"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_presence = SparkOperator(
            task_id="enriched_triceps_presence",
            name="enriched-triceps-presence",
            arguments=enriched_arguments("enriched_triceps_presence"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_participant_index >> enriched_consultation >> enriched_appointment_information >> enriched_general_information_consultation >>  [enriched_laboratory_results, enriched_pathology_results, enriched_family_history, enriched_family_history_consultation,
                                                                                                                                                  enriched_medical_history_consultation, enriched_personal_history_consultation,
                                                                                                                                                  enriched_discussion_and_plan_consultation, enriched_personal_history_consultation_table, enriched_genetic_counseling_note,
                                                                                                                                                  enriched_genetic_counseling_note_discussion_and_plan, enriched_genetic_counseling_follow_up_note, enriched_presence]

    with TaskGroup(group_id="released") as released:
        RELEASED_ZONE = "green"
        RELEASED_MAIN_CLASS = "bio.ferlab.ui.etl.green.released.versioned.Main"


        def released_arguments(destination: str) -> List[str]:
            # {{ ds }} is the DAG run’s logical date as YYYY-MM-DD. This date is used as the released version.
            return ["config/prod.conf", "default", destination, "{{ data_interval_end | ds }}"]


        released_appointment_information = SparkOperator(
            task_id="released_triceps_appointment_information",
            name="released-triceps-appointment-information",
            arguments=released_arguments("released_triceps_appointment_information"),
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
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
            spark_failure_msg=spark_failure_msg,
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
            spark_failure_msg=spark_failure_msg,
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
            spark_failure_msg=spark_failure_msg,
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
            spark_failure_msg=spark_failure_msg,
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
            spark_failure_msg=spark_failure_msg,
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
            spark_failure_msg=spark_failure_msg,
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
            spark_failure_msg=spark_failure_msg,
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
            spark_failure_msg=spark_failure_msg,
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
            spark_failure_msg=spark_failure_msg,
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
            spark_failure_msg=spark_failure_msg,
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
            spark_failure_msg=spark_failure_msg,
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
            spark_failure_msg=spark_failure_msg,
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
            spark_failure_msg=spark_failure_msg,
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
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

    with TaskGroup(group_id="published") as published:
        PUBLISHED_ZONE = "green"
        PUBLISHED_MAIN_CLASS = "bio.ferlab.ui.etl.green.published.Main"

        def published_arguments(destination: str) -> List[str]:
            return ["config/prod.conf", "default", destination]


        published_appointment_information = SparkOperator(
            task_id="published_triceps_appointment_information",
            name="published-triceps-appointment-information",
            arguments=published_arguments("published_triceps_appointment_information"),
            zone=PUBLISHED_ZONE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag,
        )

        published_consultation = SparkOperator(
            task_id="published_triceps_consultation",
            name="published-triceps-consultation",
            arguments=published_arguments("published_triceps_consultation"),
            zone=PUBLISHED_ZONE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        published_laboratory_results = SparkOperator(
            task_id="published_triceps_laboratory_results",
            name="published-triceps-laboratory-results",
            arguments=published_arguments("published_triceps_laboratory_results"),
            zone=PUBLISHED_ZONE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        published_pathology_results = SparkOperator(
            task_id="published_triceps_pathology_results",
            name="published-triceps-pathology-results",
            arguments=published_arguments("published_triceps_pathology_results"),
            zone=PUBLISHED_ZONE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        published_general_information_consultation = SparkOperator(
            task_id="published_triceps_general_information_consultation",
            name="published-triceps-general-information-consultation",
            arguments=published_arguments("published_triceps_general_information_consultation"),
            zone=PUBLISHED_ZONE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        published_family_history = SparkOperator(
            task_id="published_triceps_family_history",
            name="published-triceps-family-history",
            arguments=published_arguments("published_triceps_family_history"),
            zone=PUBLISHED_ZONE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        published_family_history_consultation = SparkOperator(
            task_id="published_triceps_family_history_consultation",
            name="published-triceps-family-history-consultation",
            arguments=published_arguments("published_triceps_family_history_consultation"),
            zone=PUBLISHED_ZONE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        published_medical_history_consultation = SparkOperator(
            task_id="published_triceps_medical_history_consultation",
            name="published-triceps-medical-history-consultation",
            arguments=published_arguments("published_triceps_medical_history_consultation"),
            zone=PUBLISHED_ZONE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        published_personal_history_consultation = SparkOperator(
            task_id="published_triceps_personal_history_consultation",
            name="published-triceps-personal-history-consultation",
            arguments=published_arguments("published_triceps_personal_history_consultation"),
            zone=PUBLISHED_ZONE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        published_discussion_and_plan_consultation = SparkOperator(
            task_id="published_triceps_discussion_and_plan_consultation",
            name="published-triceps-discussion-and-plan-consultation",
            arguments=published_arguments("published_triceps_discussion_and_plan_consultation"),
            zone=PUBLISHED_ZONE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        published_personal_history_consultation_table = SparkOperator(
            task_id="published_triceps_personal_history_consultation_table",
            name="published-triceps-personal-history-consultation-table",
            arguments=published_arguments("published_triceps_personal_history_consultation_table"),
            zone=PUBLISHED_ZONE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        published_genetic_counseling_note = SparkOperator(
            task_id="published_triceps_genetic_counseling_note",
            name="published-triceps-genetic-counseling-note",
            arguments=published_arguments("published_triceps_genetic_counseling_note"),
            zone=PUBLISHED_ZONE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        published_genetic_counseling_note_discussion_and_plan = SparkOperator(
            task_id="published_triceps_genetic_counseling_note_discussion_and_plan",
            name="published-triceps-genetic-counseling-note-discussion-and-plan",
            arguments=published_arguments("published_triceps_genetic_counseling_note_discussion_and_plan"),
            zone=PUBLISHED_ZONE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        published_genetic_counseling_follow_up_note = SparkOperator(
            task_id="published_triceps_genetic_counseling_follow_up_note",
            name="published-triceps-genetic-counseling-follow-up-note",
            arguments=published_arguments("published_triceps_genetic_counseling_follow_up_note"),
            zone=PUBLISHED_ZONE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        published_presence = SparkOperator(
            task_id="published_triceps_presence",
            name="published-triceps-presence",
            arguments=published_arguments("published_triceps_presence"),
            zone=PUBLISHED_ZONE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

    end = EmptyOperator(
        task_id="end",
        on_success_callback=Slack.notify_dag_completion
    )

    start >> enriched >> released >> published >> end
