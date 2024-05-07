"""
DAG that processes Quanum and Chartmaxx data.
"""
# pylint: disable=missing-function-docstring, duplicate-code, expression-not-assigned


from datetime import timedelta, datetime
from typing import List

import pendulum
from airflow import DAG
from airflow.decorators import task_group
from airflow.models import Param
from airflow.operators.empty import EmptyOperator

from lib.config import default_params, default_args, spark_failure_msg, jar
from lib.operators.spark import SparkOperator
from lib.tasks.notify import start, end
from spark_operators import sanitize_string

DOC = """
# Curated QuanumChartmaxx DAG
## Description
DAG that processes Quanum and Chartmaxx data. Quanum data is loaded by the IT team using Talend in our raw landing zone.
Chartmaxx data is assumed to already be processed into curated tables. If Chartmaxx data needs to be re-processed, use
`curated_chartmaxx` DAG. 

## Schedule
This DAG is scheduled to run daily at 19:00. IT loads Quanum data daily.

## Configuration
* `run_type` parameter: string indicating the type of run to perform. Possible values are `default` and `initial`. Defaults to `default`.

## Steps
This DAG will:
1. Process raw Quanum data into curated_quanum tables.
2. Union curated Chartmaxx and Quanum data into curated_quanumchartmaxx tables.
3. Anonymize curated Chartmaxx legacy tables. *These tables will stop being anonymized once project dependencies are migrated to the new quanumchartmaxx tables.*
4. Anonymized curated_quanumchartmaxx tables.
"""

QUANUM_CURATED_ZONE = "red"
QUANUMCHARTMAXX_CURATED_ZONE = "red"
QUANUMCHARTMAXX_ANONYMIZED_ZONE = "yellow"
QUANUM_CURATED_MAIN_CLASS = "bio.ferlab.ui.etl.red.curated.quanum.Main"
QUANUMCHARTMAXX_ANONYMIZED_MAIN_CLASS = "bio.ferlab.ui.etl.yellow.anonymized.Main"
QUANUMCHARTMAXX_CURATED_MAIN_CLASS = "bio.ferlab.ui.etl.red.curated.quanumchartmaxx.Main"

LOCAL_TZ = pendulum.timezone("America/Montreal")

args = default_args.copy()
args.update({
    'start_date': datetime(2023, 11, 2, tzinfo=LOCAL_TZ),
    'provide_context': True}
)

params = default_params.copy()
params.update({'run_type': Param('default', enum=['default', 'initial'])})

dag = DAG(
    dag_id="curated_quanumchartmaxx",
    doc_md=DOC,
    schedule_interval="0 19 * * *",
    start_date=datetime(2023, 11, 2, tzinfo=LOCAL_TZ),
    params=params,
    dagrun_timeout=timedelta(hours=20),
    default_args=args,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=1,
    concurrency=4,
    tags=["curated", "anonymized"]
)


def generate_spark_arguments(destination: str, steps: str = "default", etl_version: str = "v2",
                             subzone: str = "curated") -> List[str]:
    """
    Generate Spark task arguments for the ETL process.
    """
    if etl_version == "v2":
        if subzone == "curated":
            return ["config/prod.conf", steps, destination, '{{ds}}']
        return ["config/prod.conf", steps, destination]
    return [
        "--config", "config/prod.conf",
        "--steps", steps,
        "--app-name", destination,
        "--destination", destination,
    ]


def run_type() -> str:
    return '{{ params.run_type }}'


with dag:

    @task_group(group_id="curated_quanum")
    def curated_quanum():
        curated_quanum_form_data_vw_task = SparkOperator(
            task_id="curated_quanum_form_data_vw",
            name="curated_quanum_form_data_vw".replace("_", "-"),
            arguments=generate_spark_arguments("curated_quanum_form_data_vw", run_type()),
            zone=QUANUM_CURATED_ZONE,
            spark_class=QUANUM_CURATED_MAIN_CLASS,
            spark_jar=jar,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        curated_quanum_form_metadata_vw_task = SparkOperator(
            task_id="curated_quanum_form_metadata_vw",
            name="curated_quanum_form_name_vw".replace("_", "-"),
            arguments=generate_spark_arguments("curated_quanum_form_metadata_vw", run_type()),
            zone=QUANUM_CURATED_ZONE,
            spark_class=QUANUM_CURATED_MAIN_CLASS,
            spark_jar=jar,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        curated_quanum_form_name_vw_task = SparkOperator(
            task_id="curated_quanum_form_name_vw",
            name="curated_quanum_form_name_vw".replace("_", "-"),
            arguments=generate_spark_arguments("curated_quanum_form_name_vw", run_type()),
            zone=QUANUM_CURATED_ZONE,
            spark_class=QUANUM_CURATED_MAIN_CLASS,
            spark_jar=jar,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        # Make graph more readable in Airflow UI
        start_quanum_forms_task = EmptyOperator(task_id="start_quanum_forms")

        curated_quanum_config = [
            ("curated_quanum_a*", "medium-etl"),
            ("curated_quanum_b*", "small-etl"),
            ("curated_quanum_c*", "medium-etl"),
            ("curated_quanum_dossier*", "small-etl"),
            ("curated_quanum_e*", "small-etl"),
            ("curated_quanum_f*", "small-etl"),
            ("curated_quanum_g*", "small-etl"),
            ("curated_quanum_i*", "small-etl"),
            ("curated_quanum_l*", "small-etl"),
            ("curated_quanum_maladie*", "medium-etl"),
            ("curated_quanum_n*", "small-etl"),
            ("curated_quanum_o*", "small-etl"),
            ("curated_quanum_p*", "medium-etl"),
            ("curated_quanum_q*", "small-etl"),
            ("curated_quanum_r*", "small-etl"),
            ("curated_quanum_s*", "small-etl"),
            ("curated_quanum_t*", "medium-etl"),
            ("curated_quanum_urogynecologie*", "medium-etl"),
            ("curated_quanum_v*", "small-etl")
        ]

        curated_quanum_tasks = [SparkOperator(
            task_id=sanitize_string(task_name, "_"),
            name=sanitize_string(task_name[:40], '-'),
            arguments=generate_spark_arguments(task_name, run_type()),
            zone=QUANUM_CURATED_ZONE,
            spark_class=QUANUM_CURATED_MAIN_CLASS,
            spark_jar=jar,
            spark_failure_msg=spark_failure_msg,
            spark_config=cluster_size,
            dag=dag
        ) for task_name, cluster_size in curated_quanum_config]

        [curated_quanum_form_data_vw_task, curated_quanum_form_metadata_vw_task,
         curated_quanum_form_name_vw_task] >> start_quanum_forms_task
        start_quanum_forms_task >> curated_quanum_tasks


    @task_group(group_id="curated_quanumchartmaxx")
    def curated_quanumchartmaxx():
        curated_quanumchartmaxx_config = [
            ("curated_quanum_chartmaxx_a*", "medium-etl"),
            ("curated_quanum_chartmaxx_b*", "small-etl"),
            ("curated_quanum_chartmaxx_c*", "medium-etl"),
            ("curated_quanum_chartmaxx_dossier*", "small-etl"),
            ("curated_quanum_chartmaxx_e*", "small-etl"),
            ("curated_quanum_chartmaxx_f*", "small-etl"),
            ("curated_quanum_chartmaxx_g*", "small-etl"),
            ("curated_quanum_chartmaxx_i*", "small-etl"),
            ("curated_quanum_chartmaxx_l*", "small-etl"),
            ("curated_quanum_chartmaxx_maladie*", "small-etl"),
            ("curated_quanum_chartmaxx_n*", "medium-etl"),
            ("curated_quanum_chartmaxx_o*", "medium-etl"),
            ("curated_quanum_chartmaxx_p*", "medium-etl"),
            ("curated_quanum_chartmaxx_q*", "small-etl"),
            ("curated_quanum_chartmaxx_r*", "medium-etl"),
            ("curated_quanum_chartmaxx_s*", "small-etl"),
            ("curated_quanum_chartmaxx_t*", "medium-etl"),
            ("curated_quanum_chartmaxx_urogynecologie*", "medium-etl"),
            ("curated_quanum_chartmaxx_v*", "small-etl")
        ]

        [SparkOperator(
            task_id=sanitize_string(task_name, "_"),
            name=sanitize_string(task_name[:40], '-'),
            arguments=generate_spark_arguments(task_name, run_type(), "v4"),
            zone=QUANUMCHARTMAXX_CURATED_ZONE,
            spark_class=QUANUMCHARTMAXX_CURATED_MAIN_CLASS,
            spark_jar=jar,
            spark_failure_msg=spark_failure_msg,
            spark_config=cluster_size,
            dag=dag
        ) for task_name, cluster_size in curated_quanumchartmaxx_config]


    @task_group(group_id="anonymized_quanum")
    def anonymized_quanum():
        anonymized_quanum_config = [
            ("anonymized_clinique_de_pneumologie_suivi", "small-etl"),
            ("anonymized_dossier_obstetrical_*", "small-etl"),
            ("anonymized_pneumologie_consultation_initiale", "small-etl"),
        ]

        [SparkOperator(
            task_id=sanitize_string(task_name, "_"),
            name=sanitize_string(task_name[:40], '-'),
            arguments=generate_spark_arguments(task_name, run_type(), "v2", "anonymized"),
            zone=QUANUMCHARTMAXX_ANONYMIZED_ZONE,
            spark_class=QUANUMCHARTMAXX_ANONYMIZED_MAIN_CLASS,
            spark_jar=jar,
            spark_failure_msg=spark_failure_msg,
            spark_config=cluster_size,
            dag=dag
        ) for task_name, cluster_size in anonymized_quanum_config]


    @task_group(group_id="anonymized_quanumchartmaxx")
    def anonymized_quanumchartmaxx():
        anonymized_quanumchartmaxx_config = [
            ("anonymized_quanum_chartmaxx_a*", "small-etl"),
            ("anonymized_quanum_chartmaxx_childhood_asthma_test_4_11_years_old", "small-etl"),
            ("anonymized_quanum_chartmaxx_dossier*", "medium-etl"),
            ("anonymized_quanum_chartmaxx_p*", "medium-etl"),
            ("anonymized_quanum_chartmaxx_clinique_*", "medium-etl")
        ]

        [SparkOperator(
            task_id=sanitize_string(task_name, "_"),
            name=sanitize_string(task_name[:40], '-'),
            arguments=generate_spark_arguments(task_name, run_type(), "v2", "anonymized"),
            zone=QUANUMCHARTMAXX_ANONYMIZED_ZONE,
            spark_class=QUANUMCHARTMAXX_ANONYMIZED_MAIN_CLASS,
            spark_jar=jar,
            spark_failure_msg=spark_failure_msg,
            spark_config=cluster_size,
            dag=dag
        ) for task_name, cluster_size in anonymized_quanumchartmaxx_config]

    start() >> curated_quanum() >> curated_quanumchartmaxx() >> anonymized_quanum() >> anonymized_quanumchartmaxx() >> end()
