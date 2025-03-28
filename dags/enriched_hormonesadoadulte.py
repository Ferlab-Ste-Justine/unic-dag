"""
Enriched HORMONESADOADULTE DAG
"""
# pylint: disable=missing-function-docstring, duplicate-code, expression-not-assigned
from datetime import datetime, timedelta
from typing import List

import pendulum
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from lib.config import default_params, default_timeout_hours, default_args, spark_failure_msg
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.notify import end, start

JAR = 's3a://spark-prd/jars/unic-etl-{{ params.branch }}.jar'

DOC = """
# Enriched HORMONESADOADULTE DAG

### Description
ETL Enriched pour le projet HORMONESADOADULTE.
"""

# Update default args
args = default_args.copy()
args.update({'trigger_rule': TriggerRule.NONE_FAILED})

dag = DAG(
    dag_id="enriched_hormonesadoadulte",
    doc_md=DOC,
    start_date=datetime(2024, 12, 17, tzinfo=pendulum.timezone("America/Montreal")),
    params=default_params,
    schedule_interval=None,
    dagrun_timeout=timedelta(hours=default_timeout_hours),
    default_args=args,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    tags=["enriched"],
    on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
)


with dag:

    with TaskGroup(group_id="enriched") as enriched:
        ENRICHED_ZONE = "red"
        ENRICHED_MAIN_CLASS = "bio.ferlab.ui.etl.red.enriched.hormonesadoadulte.Main"

        def enriched_arguments(destination: str) -> List[str]:
            return [
                destination,
                "--config", "config/prod.conf",
                "--steps", "default",
                "--app-name", destination,
            ]

        enriched_hormonesadoadulte_participant_index = SparkOperator(
            task_id="enriched_hormonesadoadulte_participant_index",
            name="enriched-hormonesadoadulte-participant-index",
            arguments=enriched_arguments("enriched_hormonesadoadulte_participant_index") + ["--date", "{{ data_interval_end }}"],
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        enriched_hormonesadoadulte_clinic_appointment = SparkOperator(
            task_id="enriched_hormonesadoadulte_clinic_appointment",
            name="enriched-hormonesadoadulte-clinic_appointment",
            arguments=enriched_arguments("enriched_hormonesadoadulte_clinic_appointment"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        enriched_hormonesadoadulte_visited_clinic = SparkOperator(
            task_id="enriched_hormonesadoadulte_visited_clinic",
            name="enriched-hormonesadoadulte-visited_clinic",
            arguments=enriched_arguments("enriched_hormonesadoadulte_visited_clinic"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        enriched_hormonesadoadulte_selected_patients = SparkOperator(
            task_id="enriched_hormonesadoadulte_selected_patients",
            name="enriched-hormonesadoadulte-selected_patients",
            arguments=enriched_arguments("enriched_hormonesadoadulte_selected_patients"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        enriched_hormonesadoadulte_gestational_age = SparkOperator(
            task_id="enriched_hormonesadoadulte_gestational_age",
            name="enriched-hormonesadoadulte-gestational_age",
            arguments=enriched_arguments("enriched_hormonesadoadulte_gestational_age"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        enriched_hormonesadoadulte_medication = SparkOperator(
            task_id="enriched_hormonesadoadulte_medication",
            name="enriched-hormonesadoadulte-medication",
            arguments=enriched_arguments("enriched_hormonesadoadulte_medication"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        enriched_hormonesadoadulte_participant_index >> enriched_hormonesadoadulte_clinic_appointment >> enriched_hormonesadoadulte_visited_clinic >> enriched_hormonesadoadulte_selected_patients >> [enriched_hormonesadoadulte_gestational_age,
                                                                                                                                                                                                       enriched_hormonesadoadulte_medication]

    start("start_enriched_hormonesadoadulte") >> enriched >> end("end_enriched_hormonesadoadulte")
