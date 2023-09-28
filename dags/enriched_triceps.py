"""
Enriched triceps DAG
"""
# pylint: disable=missing-function-docstring, duplicate-code
from datetime import datetime, timedelta
from typing import List

import pendulum
from airflow import DAG
from airflow.models import Param, Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from core.config import default_params, default_timeout_hours, default_args, spark_failure_msg
from core.slack import Slack
from operators.spark import SparkOperator

JAR = 's3a://spark-prd/jars/unic-etl-master.jar'

DOC = """
# Enriched Triceps DAG

ETL enriched pour le projet Triceps. 

### Description
TODO.

### Horaire
* __Date de début__ - 29 septembre 2023
* __Date de fin__ - aucune
* __Jour et heure__ - Vendredi, 8h heure de Montréal
* __Intervalle__ - Chaque 4 semaine

### Configuration
* Paramètre `skip_last_visit_survey` : booléen indiquant si la table `last_visit_survey` doit être
skipped. Par défaut à True.

### Fonctionnement
La date de fin de l'intervalle (date logique du DAG) est envoyée comme argument à l'ETL released. Cette date est 
utilisée comme version de la release.
"""

# Update default params
params = default_params.copy()
params.update({"skip_last_visit_survey": Param(True, type="boolean")})
args = default_args.copy()
args.update({'trigger_rule': TriggerRule.NONE_FAILED})

dag = DAG(
    dag_id="enriched_triceps",
    doc_md=DOC,
    start_date=datetime(2023, 6, 9, 7, tzinfo=pendulum.timezone("America/Montreal")), #update for triceps
    schedule_interval=timedelta(weeks=4),
    params=params,
    dagrun_timeout=timedelta(hours=default_timeout_hours),
    default_args=args,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=1,
    max_active_tasks=3,
    tags=["enriched"]
)

with dag:
    def skip_last_visit_survey() -> str:
        return "{% if params.skip_last_visit_survey != True %}{% else %}True{% endif %}"


    start = EmptyOperator(
        task_id="start",
        on_execute_callback=Slack.notify_dag_start
    )

    with TaskGroup(group_id="enriched") as enriched:
        ENRICHED_NAMESPACE = "enriched"
        ENRICHED_MAIN_CLASS = "bio.ferlab.ui.etl.yellow.enriched.triceps.Main"

        def enriched_arguments(destination: str) -> List[str]:
            # !!! Do not set to initial, otherwise the participant index will be re-generated !!!
            return ["config/prod.conf", "default", destination, "{{ data_interval_start }}", "{{ data_interval_end }}"]


        enriched_participant_index = SparkOperator(
            task_id="enriched_triceps_participant_index",
            name="enriched-triceps-participant-index",
            arguments=enriched_arguments("enriched_triceps_participant_index"),
            namespace=ENRICHED_NAMESPACE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        enriched_appointment_information = SparkOperator(
            task_id="enriched_triceps_appointment_information",
            name="enriched-triceps-appointment-information",
            arguments=enriched_arguments("enriched_triceps_appointment_information"),
            namespace=ENRICHED_NAMESPACE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag,
        )

        enriched_consultation = SparkOperator(
            task_id="enriched_triceps_consultation",
            name="enriched-triceps-consultation",
            arguments=enriched_arguments("enriched_triceps_consultation"),
            namespace=ENRICHED_NAMESPACE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        enriched_laboratory_results = SparkOperator(
            task_id="enriched_triceps_laboratory_results",
            name="enriched-triceps-laboratory-results",
            arguments=enriched_arguments("enriched_triceps_laboratory_results"),
            namespace=ENRICHED_NAMESPACE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        enriched_pathology_results = SparkOperator(
            task_id="enriched_triceps_pathology_results",
            name="enriched-triceps-pathology-results",
            arguments=enriched_arguments("enriched_triceps_pathology_results"),
            namespace=ENRICHED_NAMESPACE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        enriched_general_information_consultation = SparkOperator(
            task_id="enriched_triceps_general_information_consultation",
            name="enriched-triceps-general-information-consultation",
            arguments=enriched_arguments("enriched_triceps_general_information_consultation"),
            namespace=ENRICHED_NAMESPACE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        enriched_family_history = SparkOperator(
            task_id="enriched_triceps_family_history",
            name="enriched-triceps-family-history",
            arguments=enriched_arguments("enriched_triceps_family_history"),
            namespace=ENRICHED_NAMESPACE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        enriched_family_history_consultation = SparkOperator(
            task_id="enriched_triceps_family_history_consultation",
            name="enriched-triceps-family-history-consultation",
            arguments=enriched_arguments("enriched_triceps_family_history_consultation"),
            namespace=ENRICHED_NAMESPACE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        enriched_medical_history_consultation = SparkOperator(
            task_id="enriched_triceps_medical_history_consultation",
            name="enriched-triceps-medical-history-consultation",
            arguments=enriched_arguments("enriched_triceps_medical_history_consultation"),
            namespace=ENRICHED_NAMESPACE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        enriched_personal_history_consultation = SparkOperator(
            task_id="enriched_triceps_personal_history_consultation",
            name="enriched-triceps-personal-history-consultation",
            arguments=enriched_arguments("enriched_triceps_personal_history_consultation"),
            namespace=ENRICHED_NAMESPACE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        enriched_discussion_and_plan_consultation = SparkOperator(
            task_id="enriched_triceps_discussion_and_plan_consultation",
            name="enriched-triceps-discussion-and-plan-consultation",
            arguments=enriched_arguments("enriched_triceps_discussion_and_plan_consultation"),
            namespace=ENRICHED_NAMESPACE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        enriched_personal_history_consultation_table = SparkOperator(
            task_id="enriched_triceps_personal_history_consultation_table",
            name="enriched-triceps-personal-history-consultation-table",
            arguments=enriched_arguments("enriched_triceps_personal_history_consultation_table"),
            namespace=ENRICHED_NAMESPACE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        enriched_genetic_counseling_note = SparkOperator(
            task_id="enriched_triceps_genetic_counseling_note",
            name="enriched-triceps-genetic-counseling-note",
            arguments=enriched_arguments("enriched_triceps_genetic_counseling_note"),
            namespace=ENRICHED_NAMESPACE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        enriched_genetic_counseling_note_discussion_and_plan = SparkOperator(
            task_id="enriched_triceps_genetic_counseling_note_discussion_and_plan",
            name="enriched-triceps-genetic-counseling-note-discussion-and-plan",
            arguments=enriched_arguments("enriched_triceps_genetic_counseling_note_discussion_and_plan"),
            namespace=ENRICHED_NAMESPACE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        enriched_genetic_counseling_follow_up_note = SparkOperator(
            task_id="enriched_triceps_genetic_counseling_follow_up_note",
            name="enriched-triceps-genetic-counseling-follow-up-note",
            arguments=enriched_arguments("enriched_triceps_genetic_counseling_follow_up_note"),
            namespace=ENRICHED_NAMESPACE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        enriched_presence = SparkOperator(
            task_id="enriched_triceps_presence",
            name="enriched-triceps-presence",
            arguments=enriched_arguments("enriched_triceps_presence"),
            namespace=ENRICHED_NAMESPACE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        enriched_participant_index >> [enriched_appointment_information, enriched_consultation, enriched_laboratory_results,
                                       enriched_pathology_results, enriched_general_information_consultation, enriched_family_history,
                                       enriched_family_history_consultation, enriched_medical_history_consultation, enriched_personal_history_consultation,
                                       enriched_discussion_and_plan_consultation, enriched_personal_history_consultation_table, enriched_genetic_counseling_note,
                                       enriched_genetic_counseling_note_discussion_and_plan, enriched_genetic_counseling_follow_up_note, enriched_presence]

    with TaskGroup(group_id="released") as released:
        RELEASED_NAMESPACE = "released"
        RELEASED_MAIN_CLASS = "bio.ferlab.ui.etl.green.released.Main"


        def released_arguments(destination: str) -> List[str]:
            # {{ ds }} is the DAG run’s logical date as YYYY-MM-DD. This date is used as the released version.
            return ["config/prod.conf", "default", destination, "{{ data_interval_end | ds }}"]


        released_appointment_information = SparkOperator(
            task_id="released_triceps_appointment_information",
            name="released-triceps-appointment-information",
            arguments=released_arguments("released_triceps_appointment_information"),
            namespace=RELEASED_NAMESPACE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag,
        )

        released_consultation = SparkOperator(
            task_id="released_triceps_consultation",
            name="released-triceps-consultation",
            arguments=released_arguments("released_triceps_consultation"),
            namespace=RELEASED_NAMESPACE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        released_laboratory_results = SparkOperator(
            task_id="released_triceps_laboratory_results",
            name="released-triceps-laboratory-results",
            arguments=released_arguments("released_triceps_laboratory_results"),
            namespace=RELEASED_NAMESPACE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        released_pathology_results = SparkOperator(
            task_id="released_triceps_pathology_results",
            name="released-triceps-pathology-results",
            arguments=released_arguments("released_triceps_pathology_results"),
            namespace=RELEASED_NAMESPACE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        released_general_information_consultation = SparkOperator(
            task_id="released_triceps_general_information_consultation",
            name="released-triceps-general-information-consultation",
            arguments=released_arguments("released_triceps_general_information_consultation"),
            namespace=RELEASED_NAMESPACE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        released_family_history = SparkOperator(
            task_id="released_triceps_family_history",
            name="released-triceps-family-history",
            arguments=released_arguments("released_triceps_family_history"),
            namespace=RELEASED_NAMESPACE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        released_family_history_consultation = SparkOperator(
            task_id="released_triceps_family_history_consultation",
            name="released-triceps-family-history-consultation",
            arguments=released_arguments("released_triceps_family_history_consultation"),
            namespace=RELEASED_NAMESPACE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        released_medical_history_consultation = SparkOperator(
            task_id="released_triceps_medical_history_consultation",
            name="released-triceps-medical-history-consultation",
            arguments=released_arguments("released_triceps_medical_history_consultation"),
            namespace=RELEASED_NAMESPACE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        released_personal_history_consultation = SparkOperator(
            task_id="released_triceps_personal_history_consultation",
            name="released-triceps-personal-history-consultation",
            arguments=released_arguments("released_triceps_personal_history_consultation"),
            namespace=RELEASED_NAMESPACE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        released_discussion_and_plan_consultation = SparkOperator(
            task_id="released_triceps_discussion_and_plan_consultation",
            name="released-triceps-discussion-and-plan-consultation",
            arguments=released_arguments("released_triceps_discussion_and_plan_consultation"),
            namespace=RELEASED_NAMESPACE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        released_personal_history_consultation_table = SparkOperator(
            task_id="released_triceps_personal_history_consultation_table",
            name="released-triceps-personal-history-consultation-table",
            arguments=released_arguments("released_triceps_personal_history_consultation_table"),
            namespace=RELEASED_NAMESPACE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        released_genetic_counseling_note = SparkOperator(
            task_id="released_triceps_genetic_counseling_note",
            name="released-triceps-genetic-counseling-note",
            arguments=released_arguments("released_triceps_genetic_counseling_note"),
            namespace=RELEASED_NAMESPACE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        released_genetic_counseling_note_discussion_and_plan = SparkOperator(
            task_id="released_triceps_genetic_counseling_note_discussion_and_plan",
            name="released-triceps-genetic-counseling-note-discussion-and-plan",
            arguments=released_arguments("released_triceps_genetic_counseling_note_discussion_and_plan"),
            namespace=RELEASED_NAMESPACE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        released_genetic_counseling_follow_up_note = SparkOperator(
            task_id="released_triceps_genetic_counseling_follow_up_note",
            name="released-triceps-genetic-counseling-follow-up-note",
            arguments=released_arguments("released_triceps_genetic_counseling_follow_up_note"),
            namespace=RELEASED_NAMESPACE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        released_presence = SparkOperator(
            task_id="released_triceps_presence",
            name="released-triceps-presence",
            arguments=released_arguments("released_triceps_presence"),
            namespace=RELEASED_NAMESPACE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

    with TaskGroup(group_id="published") as published:
        PUBLISHED_NAMESPACE = "published"
        PUBLISHED_MAIN_CLASS = "bio.ferlab.ui.etl.green.published.Main"
        mail_to = Variable.get("EMAIL_ENRICHED_SIGNATURE_MAIL_TO")


        def published_arguments(destination: str) -> List[str]:
            return ["config/prod.conf", "default", destination]


        published_appointment_information = SparkOperator(
            task_id="published_triceps_appointment_information",
            name="published-triceps-appointment-information",
            arguments=published_arguments("published_triceps_appointment_information"),
            namespace=PUBLISHED_NAMESPACE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag,
        )

        published_consultation = SparkOperator(
            task_id="published_triceps_consultation",
            name="published-triceps-consultation",
            arguments=published_arguments("published_triceps_consultation"),
            namespace=PUBLISHED_NAMESPACE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        published_laboratory_results = SparkOperator(
            task_id="published_triceps_laboratory_results",
            name="published-triceps-laboratory-results",
            arguments=published_arguments("published_triceps_laboratory_results"),
            namespace=PUBLISHED_NAMESPACE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        published_pathology_results = SparkOperator(
            task_id="published_triceps_pathology_results",
            name="published-triceps-pathology-results",
            arguments=published_arguments("published_triceps_pathology_results"),
            namespace=PUBLISHED_NAMESPACE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        published_general_information_consultation = SparkOperator(
            task_id="published_triceps_general_information_consultation",
            name="published-triceps-general-information-consultation",
            arguments=published_arguments("published_triceps_general_information_consultation"),
            namespace=PUBLISHED_NAMESPACE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        published_family_history = SparkOperator(
            task_id="published_triceps_family_history",
            name="published-triceps-family-history",
            arguments=published_arguments("published_triceps_family_history"),
            namespace=PUBLISHED_NAMESPACE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        published_family_history_consultation = SparkOperator(
            task_id="published_triceps_family_history_consultation",
            name="published-triceps-family-history-consultation",
            arguments=published_arguments("published_triceps_family_history_consultation"),
            namespace=PUBLISHED_NAMESPACE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        published_medical_history_consultation = SparkOperator(
            task_id="published_triceps_medical_history_consultation",
            name="published-triceps-medical-history-consultation",
            arguments=published_arguments("published_triceps_medical_history_consultation"),
            namespace=PUBLISHED_NAMESPACE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        published_personal_history_consultation = SparkOperator(
            task_id="published_triceps_personal_history_consultation",
            name="published-triceps-personal-history-consultation",
            arguments=published_arguments("published_triceps_personal_history_consultation"),
            namespace=PUBLISHED_NAMESPACE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        published_discussion_and_plan_consultation = SparkOperator(
            task_id="published_triceps_discussion_and_plan_consultation",
            name="published-triceps-discussion-and-plan-consultation",
            arguments=published_arguments("published_triceps_discussion_and_plan_consultation"),
            namespace=PUBLISHED_NAMESPACE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        published_personal_history_consultation_table = SparkOperator(
            task_id="published_triceps_personal_history_consultation_table",
            name="published-triceps-personal-history-consultation-table",
            arguments=published_arguments("published_triceps_personal_history_consultation_table"),
            namespace=PUBLISHED_NAMESPACE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        published_genetic_counseling_note = SparkOperator(
            task_id="published_triceps_genetic_counseling_note",
            name="published-triceps-genetic-counseling-note",
            arguments=published_arguments("published_triceps_genetic_counseling_note"),
            namespace=PUBLISHED_NAMESPACE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        published_genetic_counseling_note_discussion_and_plan = SparkOperator(
            task_id="published_triceps_genetic_counseling_note_discussion_and_plan",
            name="published-triceps-genetic-counseling-note-discussion-and-plan",
            arguments=published_arguments("published_triceps_genetic_counseling_note_discussion_and_plan"),
            namespace=PUBLISHED_NAMESPACE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        published_genetic_counseling_follow_up_note = SparkOperator(
            task_id="published_triceps_genetic_counseling_follow_up_note",
            name="published-triceps-genetic-counseling-follow-up-note",
            arguments=published_arguments("published_triceps_genetic_counseling_follow_up_note"),
            namespace=PUBLISHED_NAMESPACE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        published_presence = SparkOperator(
            task_id="published_triceps_presence",
            name="published-triceps-presence",
            arguments=published_arguments("published_triceps_presence"),
            namespace=PUBLISHED_NAMESPACE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        # with open(f"{root}/email/enriched_triceps.html", "r", encoding="utf-8") as f:
        #     html_content = f.read()
        #
        # notify = EmailOperator(
        #     task_id="notify",
        #     to=mail_to,
        #     bcc=mail_from,
        #     subject="Nouveau rapport disponible dans l'UnIC",
        #     html_content=html_content
        # )

        # [enriched_appointment_information, enriched_consultation, enriched_laboratory_results,
        #  enriched_pathology_results, enriched_general_information_consultation, enriched_family_history,
        #  enriched_family_history_consultation, enriched_medical_history_consultation, enriched_personal_history_consultation,
        #  enriched_discussion_and_plan_consultation, enriched_personal_history_consultation_table, enriched_genetic_counseling_note,
        #  enriched_genetic_counseling_note_discussion_and_plan, enriched_genetic_counseling_follow_up_note, enriched_presence] >> notify

    end = EmptyOperator(
        task_id="end",
        on_success_callback=Slack.notify_dag_completion
    )

    start >> enriched >> released >> published >> end

