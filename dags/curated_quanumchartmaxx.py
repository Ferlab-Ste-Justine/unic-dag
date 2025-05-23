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

from lib.tasks.optimize import optimize
from lib.config import DEFAULT_PARAMS, DEFAULT_ARGS, SPARK_FAILURE_MSG, JAR, CONFIG_FILE
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.notify import start, end
from lib.utils import sanitize_string

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
QUANUM_CURATED_NEW_FORM_CHECKER_CLASS = "bio.ferlab.ui.etl.script.QuanumNewFormChecker"

LOCAL_TZ = pendulum.timezone("America/Montreal")

args = DEFAULT_ARGS.copy()
args.update({
    'start_date': datetime(2023, 11, 2, tzinfo=LOCAL_TZ),
    'provide_context': True}
)

params = DEFAULT_PARAMS.copy()
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
    tags=["curated", "anonymized"],
    on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
)


def generate_spark_arguments(destination: str, pass_date: bool, steps: str = "default") -> List[str]:
    """
    Generate Spark task arguments for the ETL process.
    """
    arguments = [
        "--config", CONFIG_FILE,
        "--steps", steps,
        "--app-name", destination,
        "--destination", destination,
    ]

    if pass_date:
        arguments.append("--date")
        arguments.append("{{ ds }}")

    return arguments


def run_type() -> str:
    return '{{ params.run_type }}'


with dag:
    @task_group(group_id="curated_quanum")
    def curated_quanum():
        curated_quanum_form_data_vw_task = SparkOperator(
            task_id="curated_quanum_form_data_vw",
            name="curated_quanum_form_data_vw".replace("_", "-"),
            arguments=generate_spark_arguments("curated_quanum_form_data_vw", pass_date=True, steps=run_type()),
            zone=QUANUM_CURATED_ZONE,
            spark_class=QUANUM_CURATED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="small-etl",
            dag=dag
        )

        curated_quanum_form_metadata_vw_task = SparkOperator(
            task_id="curated_quanum_form_metadata_vw",
            name="curated_quanum_form_metadata_vw".replace("_", "-"),
            arguments=generate_spark_arguments("curated_quanum_form_metadata_vw", pass_date=True, steps=run_type()),
            zone=QUANUM_CURATED_ZONE,
            spark_class=QUANUM_CURATED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="small-etl",
            dag=dag
        )

        curated_quanum_form_name_vw_task = SparkOperator(
            task_id="curated_quanum_form_name_vw",
            name="curated_quanum_form_name_vw".replace("_", "-"),
            arguments=generate_spark_arguments("curated_quanum_form_name_vw", pass_date=True, steps=run_type()),
            zone=QUANUM_CURATED_ZONE,
            spark_class=QUANUM_CURATED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="small-etl",
            dag=dag
        )

        # Check if there are new forms that we don't have in etl ds configuration
        curated_quanum_new_form_checker_task = SparkOperator(
            task_id="curated_quanum_new_form_checker",
            name="curated_quanum_new_form_checker".replace("_", "-"),
            arguments=["config/prod.conf", "default"],
            zone=QUANUM_CURATED_ZONE,
            spark_class=QUANUM_CURATED_NEW_FORM_CHECKER_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="xsmall-etl",
            dag=dag
        )

        # Make graph more readable in Airflow UI
        start_quanum_forms_task = EmptyOperator(task_id="start_quanum_forms")

        curated_quanum_config = [
            ("curated_quanum_a*", "medium-etl"),
            ("curated_quanum_c*", "medium-etl"),
            ("curated_quanum_d*", "small-etl"),
            ("curated_quanum_e*", "medium-etl"),
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
            arguments=generate_spark_arguments(task_name, pass_date=True, steps=run_type()),
            zone=QUANUM_CURATED_ZONE,
            spark_class=QUANUM_CURATED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config=cluster_size,
            dag=dag
        ) for task_name, cluster_size in curated_quanum_config]

        curated_quanum_optimization_task = optimize(['curated_quanum_form_metadata_vw', 'curated_quanum_form_name_vw']
                                                    , "quanum", QUANUM_CURATED_ZONE, "curated", CONFIG_FILE, JAR, dag)

        [curated_quanum_form_data_vw_task, curated_quanum_form_metadata_vw_task,
         curated_quanum_form_name_vw_task] >> curated_quanum_new_form_checker_task >> start_quanum_forms_task
        start_quanum_forms_task >> curated_quanum_tasks >> curated_quanum_optimization_task


    @task_group(group_id="curated_quanumchartmaxx")
    def curated_quanumchartmaxx():
        curated_quanumchartmaxx_config = [
            ("curated_quanum_chartmaxx_a*", "medium-etl"),
            ("curated_quanum_chartmaxx_b*", "small-etl"),
            ("curated_quanum_chartmaxx_c*", "medium-etl"),
            ("curated_quanum_chartmaxx_d*", "small-etl"),
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

        curated_quanum_chartmaxx_tasks = [SparkOperator(
            task_id=sanitize_string(task_name, "_"),
            name=sanitize_string(task_name[:40], '-'),
            arguments=generate_spark_arguments(task_name, pass_date=False, steps=run_type()),
            zone=QUANUMCHARTMAXX_CURATED_ZONE,
            spark_class=QUANUMCHARTMAXX_CURATED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config=cluster_size,
            dag=dag
        ) for task_name, cluster_size in curated_quanumchartmaxx_config]

        curated_quanum_chartmaxx_optimization_task = optimize(
            ["curated_quanum_chartmaxx_a*", "curated_quanum_chartmaxx_b*", "curated_quanum_chartmaxx_c*",
             "curated_quanum_chartmaxx_d*",
             "curated_quanum_chartmaxx_e*", "curated_quanum_chartmaxx_fibrose_kystique_depistage_neonatal",
             "curated_quanum_chartmaxx_fkp_depistage_neonat_1ere_consultation",
             "curated_quanum_chartmaxx_g*", "curated_quanum_chartmaxx_i*", "curated_quanum_chartmaxx_l*",
             "curated_quanum_chartmaxx_maladie*", "curated_quanum_chartmaxx_n*",
             "curated_quanum_chartmaxx_o*", "curated_quanum_chartmaxx_p*", "curated_quanum_chartmaxx_q*",
             "curated_quanum_chartmaxx_r*", "curated_quanum_chartmaxx_s*",
             "curated_quanum_chartmaxx_t*", "curated_quanum_chartmaxx_urogynecologie*", "curated_quanum_chartmaxx_v*"],
            "quanum_chartmaxx", QUANUMCHARTMAXX_CURATED_ZONE, "curated", CONFIG_FILE, JAR, dag)

        curated_quanum_chartmaxx_tasks >> curated_quanum_chartmaxx_optimization_task


    @task_group(group_id="anonymized_quanum")
    def anonymized_quanum():
        anonymized_quanum_config = [
            ("anonymized_quanum_clinique_de_pneumologie_suivi", "small-etl"),
            ("anonymized_quanum_dossier_obstetrical_*", "small-etl"),
            ("anonymized_quanum_pneumologie_consultation_initiale", "small-etl"),
        ]

        anonymized_quanum_tasks = [SparkOperator(
            task_id=sanitize_string(task_name, "_"),
            name=sanitize_string(task_name[:40], '-'),
            arguments=["config/prod.conf", run_type(), task_name],
            zone=QUANUMCHARTMAXX_ANONYMIZED_ZONE,
            spark_class=QUANUMCHARTMAXX_ANONYMIZED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config=cluster_size,
            dag=dag
        ) for task_name, cluster_size in anonymized_quanum_config]

        anonymized_quanum_optimization_tasks = optimize(['anonymized_quanum_*'], "quanum",
                                                        QUANUMCHARTMAXX_ANONYMIZED_ZONE, "anonymized", CONFIG_FILE, JAR, dag)

        anonymized_quanum_tasks >> anonymized_quanum_optimization_tasks


    @task_group(group_id="anonymized_quanumchartmaxx")
    def anonymized_quanumchartmaxx():
        anonymized_quanumchartmaxx_config = [
            ("anonymized_quanum_chartmaxx_a*", "small-etl"),
            ("anonymized_quanum_chartmaxx_childhood_asthma_test_4_11_years_old", "small-etl"),
            ("anonymized_quanum_chartmaxx_dossier*", "medium-etl"),
            ("anonymized_quanum_chartmaxx_p*", "medium-etl"),
            ("anonymized_quanum_chartmaxx_clinique_*", "medium-etl")
        ]

        anonymized_quanum_chartmaxx_tasks = [SparkOperator(
            task_id=sanitize_string(task_name, "_"),
            name=sanitize_string(task_name[:40], '-'),
            arguments=["config/prod.conf", run_type(), task_name],
            zone=QUANUMCHARTMAXX_ANONYMIZED_ZONE,
            spark_class=QUANUMCHARTMAXX_ANONYMIZED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config=cluster_size,
            dag=dag
        ) for task_name, cluster_size in anonymized_quanumchartmaxx_config]

        anonymized_quanum_chartmaxx_optimization_tasks = optimize(
            ['anonymized_quanum_chartmaxx*'], "quanum_chartmaxx", QUANUMCHARTMAXX_ANONYMIZED_ZONE, "anonymized",
            CONFIG_FILE, JAR, dag)

        anonymized_quanum_chartmaxx_tasks >> anonymized_quanum_chartmaxx_optimization_tasks


    start() >> curated_quanum() >> curated_quanumchartmaxx() >> anonymized_quanum() >> anonymized_quanumchartmaxx() >> end()
