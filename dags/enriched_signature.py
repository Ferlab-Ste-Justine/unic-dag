"""
Enriched signature DAG
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
# Enriched Signature DAG

ETL enriched pour le projet Signature. 

### Description
Cet ETL génère un rapport mensuel sur les patients de l'étude ayant eu une ordonnance pour un test de laboratoire
depuis les quatre dernières semaines. Par défaut, les tâches liées à la table `last_visit_survey`, qui contient les
données depuis le début de l'étude, ne sont pas exécutées.

### Horaire
* __Date de début__ - 7 juillet 2023
* __Date de fin__ - aucune
* __Jour et heure__ - Vendredi, 7h heure de Montréal
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
params = default_params.copy()
params.update({"skip_last_visit_survey": Param(True, type="boolean")})
args = default_args.copy()
args.update({'trigger_rule': TriggerRule.NONE_FAILED})

dag = DAG(
    dag_id="enriched_signature",
    doc_md=DOC,
    start_date=datetime(2023, 6, 9, 7, tzinfo=pendulum.timezone("America/Montreal")),
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
        ENRICHED_MAIN_CLASS = "bio.ferlab.ui.etl.yellow.enriched.signature.Main"


        def enriched_arguments(destination: str) -> List[str]:
            # !!! Do not set to initial, otherwise the participant index will be re-generated !!!
            return ["config/prod.conf", "default", destination, "{{ data_interval_start }}", "{{ data_interval_end }}"]


        enriched_participant_index = SparkOperator(
            task_id="enriched_signature_participant_index",
            name="enriched-signature-participant-index",
            arguments=enriched_arguments("enriched_signature_participant_index"),
            namespace=ENRICHED_NAMESPACE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        enriched_last_visit_survey = SparkOperator(
            task_id="enriched_signature_last_visit_survey",
            name="enriched-signature-last-visit-survey",
            arguments=enriched_arguments("enriched_signature_last_visit_survey"),
            namespace=ENRICHED_NAMESPACE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag,
            skip=skip_last_visit_survey()
        )

        enriched_monthly_visit = SparkOperator(
            task_id="enriched_signature_monthly_visit",
            name="enriched-signature-monthly-visit",
            arguments=enriched_arguments("enriched_signature_monthly_visit"),
            namespace=ENRICHED_NAMESPACE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_participant_index >> [enriched_last_visit_survey, enriched_monthly_visit]

    with TaskGroup(group_id="released") as released:
        RELEASED_NAMESPACE = "released"
        RELEASED_MAIN_CLASS = "bio.ferlab.ui.etl.green.released.Main"


        def released_arguments(destination: str) -> List[str]:
            # {{ ds }} is the DAG run’s logical date as YYYY-MM-DD. This date is used as the released version.
            return ["config/prod.conf", "default", destination, "{{ data_interval_end | ds }}"]


        released_last_visit_survey = SparkOperator(
            task_id="released_signature_last_visit_survey",
            name="released-signature-last-visit-survey",
            arguments=released_arguments("released_signature_last_visit_survey"),
            namespace=RELEASED_NAMESPACE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag,
            skip=skip_last_visit_survey()
        )

        released_monthly_visit = SparkOperator(
            task_id="released_signature_monthly_visit",
            name="released-signature-monthly-visit",
            arguments=released_arguments("released_signature_monthly_visit"),
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


        published_last_visit_survey = SparkOperator(
            task_id="published_signature_last_visit_survey",
            name="published-signature-last-visit-survey",
            arguments=published_arguments("published_signature_last_visit_survey"),
            namespace=PUBLISHED_NAMESPACE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="xsmall-etl",
            dag=dag,
            skip=skip_last_visit_survey()
        )

        published_monthly_visit = SparkOperator(
            task_id="published_signature_monthly_visit",
            name="published-signature-monthly-visit",
            arguments=published_arguments("published_signature_monthly_visit"),
            namespace=PUBLISHED_NAMESPACE,
            spark_class=PUBLISHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="xsmall-etl",
            dag=dag
        )

        # with open(f"{root}/email/enriched_signature.html", "r", encoding="utf-8") as f:
        #     html_content = f.read()
        #
        # notify = EmailOperator(
        #     task_id="notify",
        #     to=mail_to,
        #     bcc=mail_from,
        #     subject="Nouveau rapport disponible dans l'UnIC",
        #     html_content=html_content
        # )

        # [published_last_visit_survey, published_monthly_visit] >> notify

    end = EmptyOperator(
        task_id="end",
        on_success_callback=Slack.notify_dag_completion
    )

    start >> enriched >> released >> published >> end
