"""
Enriched signature DAG
"""
# pylint: disable=missing-function-docstring, duplicate-code
from datetime import datetime, timedelta
from typing import List

import pendulum
from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from core.config import default_params, default_timeout_hours, default_args, spark_failure_msg
from core.slack import Slack
from operators.spark import SparkOperator

NAMESPACE = "enriched"
MAIN_CLASS = "bio.ferlab.ui.etl.yellow.enriched.signature.Main"
JAR = 's3a://spark-prd/jars/unic-etl-unic-1133.jar'

DOC = """
# Enriched Signature DAG

ETL enriched pour le projet Signature. 

### Description
Cet ETL génère un rapport mensuel sur les patients de l'étude ayant eu une ordonnance pour un test de laboratoire
depuis les quatre dernières semaines. ~~Par défaut, la task `enriched_signature_last_visit_survey`, qui génère un rapport
depuis le début de l'étude, n'est pas exécutée.~~

### Horaire
* __Date de début__ - 7 juillet 2023
* __Date de fin__ - aucune
* __Jour et heure__ - Vendredi, 7h heure de Montréal
* __Intervalle__ - Chaque 4 semaine

### Configuration
* Paramètre `skip_last_visit_survey` : booléen indiquant si la task `enriched_signature_last_visit_survey` doit être
skipped. ~~Par défaut à True.~~

### Fonctionnement
Le début de l'intervalle et la fin de l'intervalle sont envoyés comme arguments à l'ETL. Seule la tâche 
`enriched_signature_monthly_visit` a besoin de ces arguments pour filtrer les ordonnances de laboratoire. À noter que
la fin de l'intervalle correspond au moment de génération du rapport. Donc pour le premier rapport du 7 juillet 2023, le
début de l'intervalle est le 9 juin 2023. 
"""

# Update default params
params = default_params.copy()
params.update({"skip_last_visit_survey": Param(False, type="boolean")})  # Set to False for first run
args = default_args.copy()
args.update({'trigger_rule': TriggerRule.NONE_FAILED})

dag = DAG(
    dag_id="enriched_signature",
    doc_md=DOC,
    start_date=datetime(2023, 5, 12, 7, tzinfo=pendulum.timezone("America/Montreal")),
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
    def arguments(destination: str) -> List[str]:
        return ["config/prod.conf", "initial", destination, "{{ data_interval_start }}", "{{ data_interval_end }}"]


    def skip_last_visit_survey() -> str:
        return "{% if params.skip_last_visit_survey != 'True' %}{% else %}True{% endif %}"

    start = EmptyOperator(
        task_id="start_enriched_signature",
        on_execute_callback=Slack.notify_dag_start
    )

    participant_index = SparkOperator(
        task_id="enriched_signature_participant_index",
        name="enriched-signature-participant-index",
        arguments=arguments("enriched_signature_participant_index"),
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=spark_failure_msg,
        spark_config="small-etl",
        dag=dag
    )

    last_visit_survey = SparkOperator(
        task_id="enriched_signature_last_visit_survey",
        name="enriched-signature-last-visit-survey",
        arguments=arguments("enriched_signature_last_visit_survey"),
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=spark_failure_msg,
        spark_config="medium-etl",
        dag=dag,
        skip=skip_last_visit_survey()
    )

    monthly_visit = SparkOperator(
        task_id="enriched_signature_monthly_visit",
        name="enriched-signature-monthly-visit",
        arguments=arguments("enriched_signature_monthly_visit"),
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=spark_failure_msg,
        spark_config="medium-etl",
        dag=dag
    )

    end = EmptyOperator(
        task_id="publish_enriched_signature",
        on_success_callback=Slack.notify_dag_completion
    )

    start >> participant_index >> [last_visit_survey, monthly_visit] >> end
