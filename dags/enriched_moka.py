"""
Enriched MoKa DAG
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
# Enriched Moka DAG

ETL enriched pour le projet Moka. 

### Description
Cet ETL génère un rapport mensuel pour la santé mobile chez les enfants atteints d'asthme.

### Horaire
* __Date de début__ - 20 octobre 2023
* __Date de fin__ - aucune
* __Jour et heure__ - Vendredi, 8h heure de Montréal
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
args = default_args.copy()
args.update({'trigger_rule': TriggerRule.NONE_FAILED})

dag = DAG(
    dag_id="enriched_moka",
    doc_md=DOC,
    start_date=datetime(2023, 10, 20, 8, tzinfo=pendulum.timezone("America/Montreal")),
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
        ENRICHED_ZONE = "red"
        ENRICHED_MAIN_CLASS = "bio.ferlab.ui.etl.red.enriched.moka.Main"

        def enriched_arguments(destination: str) -> List[str]:
            # !!! Do not set to initial, otherwise the participant index will be re-generated !!!
            return ["config/prod.conf", "default", destination, "{{ data_interval_start }}", "{{ data_interval_end }}"]


        enriched_participant_index = SparkOperator(
            task_id="enriched_moka_participant_index",
            name="enriched-moka-participant-index",
            arguments=enriched_arguments("enriched_moka_participant_index"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        enriched_screening = SparkOperator(
            task_id="enriched_moka_screening",
            name="enriched-moka-screening",
            arguments=enriched_arguments("enriched_moka_screening"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag,
        )

        enriched_participant_index >> enriched_screening

    end = EmptyOperator(
        task_id="end",
        on_success_callback=Slack.notify_dag_completion
    )

    start >> enriched >> end
