"""
DAG : checks if there are new tables available in the Centro schema in the integration db
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param

from core.config import default_args, jar, spark_failure_msg
from core.slack import Slack
from operators.spark import SparkOperator

ZONE = "red"
POD_NAME = "raw-schema-diff-checker"
MAIN_CLASS = "bio.ferlab.ui.etl.script.SchemaDiffChecker"
ARGS = ["config/prod.conf", "initial", "dbschema"]

slack = Slack()

dag = DAG(
    dag_id="schema_diff_checker",
    start_date=datetime(2022, 2, 8),
    schedule_interval="@weekly",
    params={
        "branch":  Param("master", type="string"),
        "version": Param("latest", type="string")
    },
    dagrun_timeout=timedelta(hours=1),
    default_args=default_args,
    tags=["script"],
    is_paused_upon_creation=True,
    catchup=False,
)

check_schema_difference = SparkOperator(
    task_id="check_schema_difference",
    name=POD_NAME,
    arguments=default_args,
    zone=ZONE,
    spark_class=MAIN_CLASS,
    spark_jar=jar,
    spark_failure_msg=spark_failure_msg,
    spark_config="xsmall-etl",
    dag=dag,
)

check_schema_difference
