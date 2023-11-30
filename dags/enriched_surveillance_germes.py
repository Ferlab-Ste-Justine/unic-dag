"""
Enriched Surveillance Germes DAG
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
# Enriched Surveillance Germes DAG

ETL enriched pour le projet Surveillance Germes. 

### Description
Cet ETL génère un rapport hebdomadaire sur les patients ayant eu un test positif pour une liste de pathogènes dans la 
dernière semaine, du dimanche au samedi. Le rapport sera livré tous les vendredis. La toute première exécution doit contenir 
les données historiques par semaine à partir du 1er novembre 2023, après cette date, le rapport devient hebdomadaire. 
Une mise à jour sera faite et d'autres rapports s'ajouteront lorsque le chercheur aura reçu les autorisations éthiques.

### Horaire
* __Date de début__ - 10 novembre 2023
* __Date de fin__ - aucune
* __Jour et heure__ - Vendredi, 6h heure de Montréal
* __Intervalle__ - Chaque semaine

### Fonctionnement
la date logique du DAG est envoyé comme argument à l'ETL enriched. L'intervalle est calculé à partir de cette date et 
correspond à la période du Dimanche au Samedi pécédent. Donc pour le premier rapport du 10 Novembre 2023, l'intervalle 
est du 29 Octobre au 4 Novembre 2023. 

La date de livraison (la date logique du DAG) est envoyée comme argument à l'ETL released. Cette date est 
utilisée comme version de la release.
"""

# Update default args
args = default_args.copy()
args.update({'trigger_rule': TriggerRule.NONE_FAILED})

dag = DAG(
    dag_id="enriched_surveillance_germes",
    doc_md=DOC,
    start_date=datetime(2023, 11, 10, 6, tzinfo=pendulum.timezone("America/Montreal")),
    schedule_interval=timedelta(weeks=1),
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

    start = EmptyOperator(
        task_id="start",
        on_execute_callback=Slack.notify_dag_start
    )

    with TaskGroup(group_id="enriched") as enriched:
        ENRICHED_ZONE = "yellow"
        ENRICHED_MAIN_CLASS = "bio.ferlab.ui.etl.yellow.enriched.sil.surveillancegermes"

        def enriched_arguments(destination: str) -> List[str]:
            # !!! Do not set to initial, otherwise the participant index will be re-generated !!!
            return ["config/prod.conf", "default", destination, "{{ ds }}"]


        enriched_patient = SparkOperator(
            task_id="enriched_surveillancegermes_patient",
            name="enriched-surveillancegermes-patient",
            arguments=enriched_arguments("enriched_surveillancegermes_patient"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_weekly_summary = SparkOperator(
            task_id="enriched_surveillancegermes_weekly_summary",
            name="enriched-surveillancegermes-weekly-summary",
            arguments=enriched_arguments("enriched_surveillancegermes_weekly_summary"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag,
        )

        enriched_patient >> enriched_weekly_summary

    with TaskGroup(group_id="released") as released:
        RELEASED_ZONE = "green"
        RELEASED_MAIN_CLASS = "bio.ferlab.ui.etl.green.released.Main"

        def released_arguments(destination: str) -> List[str]:
            # {{ ds }} is the DAG run’s logical date as YYYY-MM-DD. This date is used as the released version.
            return ["config/prod.conf", "default", destination, "{{ ds }}"]


        released_weekly_summary = SparkOperator(
            task_id="released_surveillancegermes_weekly_summary",
            name="released-surveillancegermes-weekly-summary",
            arguments=released_arguments("released_surveillancegermes_weekly_summary"),
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag,
        )

    with TaskGroup(group_id="published") as published:
        PUBLISHED_ZONE = "green"
        PUBLISHED_MAIN_CLASS = "bio.ferlab.ui.etl.green.published.Main"

        def published_arguments(destination: str) -> List[str]:
            return ["config/prod.conf", "default", destination]

        published_weekly_summary = SparkOperator(
            task_id="published_surveillancegermes_weekly_summary",
            name="published-surveillancegermes-weekly-summary",
            arguments=published_arguments("published_surveillancegermes_weekly_summary"),
            zone=PUBLISHED_ZONE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag,
        )

    end = EmptyOperator(
        task_id="end",
        on_success_callback=Slack.notify_dag_completion
    )

    start >> enriched >> released >> published >> end
