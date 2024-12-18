"""
Enriched promptbolus DAG
"""
# pylint: disable=missing-function-docstring, duplicate-code, expression-not-assigned
from datetime import datetime, timedelta
from typing import List

import pendulum
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from lib.config import default_params, default_timeout_hours, default_args, spark_failure_msg, version
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.notify import end, start

JAR = 's3a://spark-prd/jars/unic-etl-master.jar'

DOC = """
# Enriched promptbolus DAG

ETL enriched pour le projet Prompt Bolus. 

### Description
Cet ETL génère un rapport aux 2 semaines afin d'identifier les patients ayant eu 1 antibiotiques et 2 bolus lors
de leur visite à l'urgence. Un premier rapport historique du 1er septembre au 30 novembre 2024 doit être généré.

### Horaire
* __Date de début__ - 30 octobre 2024
* __Date de fin__ - aucune
* __Jour et heure__ - Mercredi, 14h heure de Montréal
* __Intervalle__ - Chaque 2 semaines

### Configuration

### Fonctionnement
Le début de l'intervalle et la fin de l'intervalle sont envoyés comme arguments à l'ETL enriched. Seule la tâche 
`enriched_promptbolus_participant_index` a besoin de ces arguments pour filtrer les visites à l'urgence. 

La date de fin de l'intervalle (date logique du DAG) est envoyée comme argument à l'ETL released. Cette date est 
utilisée comme version de la release.
"""

# Update default params
params = default_params.copy()
args = default_args.copy()
args.update({'trigger_rule': TriggerRule.NONE_FAILED})

dag = DAG(
    dag_id="enriched_promptbolus",
    doc_md=DOC,
    start_date=datetime(2024, 8, 28, 14, tzinfo=pendulum.timezone("America/Montreal")),
    schedule_interval=timedelta(weeks=2),
    params=params,
    dagrun_timeout=timedelta(hours=default_timeout_hours),
    default_args=args,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=1,
    max_active_tasks=3,
    tags=["enriched"],
    on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
)

with dag:

    with TaskGroup(group_id="enriched") as enriched:
        ENRICHED_ZONE = "yellow"
        ENRICHED_MAIN_CLASS = "bio.ferlab.ui.etl.yellow.enriched.promptbolus.Main"

        def enriched_arguments(destination: str) -> List[str]:
            # !!! Do not set to initial, otherwise the participant index will be re-generated !!!
            return ["config/prod.conf", "default", destination, "{{ data_interval_start }}", "{{ data_interval_end }}"]


        enriched_participant_index = SparkOperator(
            task_id="enriched_promptbolus_participant_index",
            name="enriched-promptbolus-participant-index",
            arguments=enriched_arguments("enriched_promptbolus_participant_index"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        enriched_patients = SparkOperator(
            task_id="enriched_promptbolus_patients",
            name="enriched-promptbolus-patients",
            arguments=enriched_arguments("enriched_promptbolus_patients"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        enriched_antibiotics_received = SparkOperator(
            task_id="enriched_promptbolus_antibiotics_received",
            name="enriched-promptbolus-antibiotics-received",
            arguments=enriched_arguments("enriched_promptbolus_antibiotics_received"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        enriched_bolus_received = SparkOperator(
            task_id="enriched_promptbolus_bolus_received",
            name="enriched-promptbolus-bolus-received",
            arguments=enriched_arguments("enriched_promptbolus_bolus_received"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        enriched_participant_index >> [enriched_patients, enriched_antibiotics_received, enriched_bolus_received]

    with TaskGroup(group_id="released") as released:
        RELEASED_ZONE = "green"
        RELEASED_MAIN_CLASS = "bio.ferlab.ui.etl.green.released.versioned.Main"


        def released_arguments(destination: str) -> List[str]:
            # {{ ds }} is the DAG run’s logical date as YYYY-MM-DD. This date is used as the released version.
            return ["config/prod.conf", "default", destination, "{{ data_interval_end | ds }}"]


        released_patients = SparkOperator(
            task_id="released_promptbolus_patients",
            name="released-promptbolus-patients",
            arguments=released_arguments("released_promptbolus_patients"),
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        released_antibiotics_received = SparkOperator(
            task_id="released_promptbolus_antibiotics_received",
            name="released-promptbolus-antibiotics-received",
            arguments=released_arguments("released_promptbolus_antibiotics_received"),
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        released_bolus_received = SparkOperator(
            task_id="released_promptbolus_bolus_received",
            name="released-promptbolus-bolus-received",
            arguments=released_arguments("released_promptbolus_bolus_received"),
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

    with TaskGroup(group_id="published") as published:
        PUBLISHED_ZONE = "green"
        PUBLISHED_MAIN_CLASS = "bio.ferlab.ui.etl.green.published.Main"


        def published_arguments(destination: str) -> List[str]:
            return ["config/prod.conf", "default", destination, version]


        published_patients = SparkOperator(
            task_id="published_promptbolus_patients",
            name="published-promptbolus-patients",
            arguments=published_arguments("published_promptbolus_patients"),
            zone=PUBLISHED_ZONE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="xsmall-etl",
            dag=dag
        )

        published_antibiotics_received = SparkOperator(
            task_id="published_promptbolus_antibiotics_received",
            name="published-promptbolus-antibiotics-received",
            arguments=published_arguments("published_promptbolus_antibiotics_received"),
            zone=PUBLISHED_ZONE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="xsmall-etl",
            dag=dag
        )

        published_bolus_received = SparkOperator(
            task_id="published_promptbolus_bolus_received",
            name="published-promptbolus-bolus-received",
            arguments=published_arguments("published_promptbolus_bolus_received"),
            zone=PUBLISHED_ZONE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="xsmall-etl",
            dag=dag
        )

    start() >> enriched >> released >> published >> end()
