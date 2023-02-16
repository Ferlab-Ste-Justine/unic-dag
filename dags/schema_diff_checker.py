"""
DAG : checks if there are new tables available in the Centro schema in the integration db
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param

from core.default_args import generate_default_args
from core.slack import Slack
from operators.spark import SparkOperator

args = ["config/prod.conf", "initial", "dbschema"]

NAMESPACE = "raw"
POD_NAME = "raw-schema-diff-checker"

MAIN_CLASS = "bio.ferlab.ui.etl.script.SchemaDiffChecker"

JAR = 's3a://spark-prd/jars/unic-etl-{{ params.branch }}.jar'

default_args = generate_default_args(owner="unic", on_failure_callback=Slack.notify_task_failure)

slack = Slack()

dag = DAG(
    dag_id="schema_diff_checker",
    start_date=datetime(2022, 2, 8),
    schedule_interval="@weekly",
    params={
        "branch":  Param("UNIC-875", type="string"),
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
    arguments=args,
    namespace=NAMESPACE,
    spark_class=MAIN_CLASS,
    spark_jar=JAR,
    spark_config="xsmall-etl",
    dag=dag,
)

check_schema_difference
