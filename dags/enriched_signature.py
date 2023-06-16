from datetime import datetime, timedelta
from typing import List

import pendulum
from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator

from core.config import default_params, default_timeout_hours, default_args, jar, spark_failure_msg
from core.slack import Slack
from operators.spark import SparkOperator

NAMESPACE = "raw"
MAIN_CLASS = "bio.ferlab.ui.etl.yellow.enriched.signature.Main"

# Add extra param
params = default_params.copy()
params.update({"skip_last_visit_survey": Param(True, type="boolean")})

dag = DAG(
    dag_id="enriched_signature",
    start_date=datetime(2023, 5, 19, 7, tzinfo=pendulum.timezone("America/Montreal")),
    schedule_interval=timedelta(weeks=4),
    params=params,
    dagrun_timeout=timedelta(hours=default_timeout_hours),
    default_args=default_args,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=1,
    max_active_tasks=3,
    tags=["enriched"]
)

with dag:
    def arguments(destination: str) -> List[str]:
        return ["config/prod.conf", "default", destination, "{{ data_interval_start }}", "{{ data_interval_end }}"]


    def skip_last_visit_survey() -> bool:
        return "{{ params.last_visit_survey }}" == "True"


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
        spark_jar=jar,
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
        spark_jar=jar,
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
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="medium-etl",
        dag=dag
    )

    end = EmptyOperator(
        task_id="publish_enriched_signature",
        on_success_callback=Slack.notify_dag_completion
    )

    start >> participant_index >> [last_visit_survey, monthly_visit] >> end
