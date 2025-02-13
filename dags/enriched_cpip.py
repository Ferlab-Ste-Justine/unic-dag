"""
Enriched CPIP DAG
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
# Enriched CPIP DAG

### Description
ETL Enriched pour le projet CPIP.
"""

# Update default args
args = default_args.copy()
args.update({'trigger_rule': TriggerRule.NONE_FAILED})

dag = DAG(
    dag_id="enriched_cpip",
    doc_md=DOC,
    start_date=datetime(2024, 7, 23, tzinfo=pendulum.timezone("America/Montreal")),
    schedule=None,
    params=default_params,
    dagrun_timeout=timedelta(hours=default_timeout_hours),
    default_args=args,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=1,
    max_active_tasks=2,
    tags=["enriched"],
    on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
)


with dag:

    with TaskGroup(group_id="enriched") as enriched:
        ENRICHED_ZONE = "red"
        ENRICHED_MAIN_CLASS = "bio.ferlab.ui.etl.red.enriched.cpip.ParticipantIndexETL"

        def enriched_arguments(destination: str) -> List[str]:
            return [
                "--config", "config/prod.conf",
                "--steps", "default",
                "--app-name", destination,
                "--date", "{{ data_interval_end | ds }}"
            ]

        enriched_cpip_participant_index = SparkOperator(
            task_id="enriched_cpip_participant_index",
            name="enriched-cpip-participant-index",
            arguments=enriched_arguments("enriched_cpip_participant_index"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        enriched_cpip_participant_index

    start("start_enriched_cpip") >> enriched >> end("end_enriched_cpip")
