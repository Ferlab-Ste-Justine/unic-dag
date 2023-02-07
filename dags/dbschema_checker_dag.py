from airflow import DAG
from core.slack import Slack
from datetime import datetime, timedelta
from operators.spark import SparkOperator
from core.default_args import generate_default_args
from airflow.operators.python import PythonOperator

from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

args = ["config/prod.conf"]

namespace = "raw"
pod_name = "raw-schema-diff-checker"

main_class = "bio.ferlab.ui.etl.script.SchemaDiffLookup"

jar = 's3a://spark-prd/jars/unic-etl-{{ params.branch }}.jar'
default_args = generate_default_args(owner="jamine", on_failure_callback=Slack.notify_task_failure)

slack = Slack()

dag = DAG(
    "schema_diff_checker",
    start_date=datetime(2022, 2, 8),
    default_args=default_args,
    schedule_interval=timedelta(days=7),
    tags=["dbschema"],
)

check_schema_difference = SparkOperator(
    task_id="check_schema_difference",
    name=pod_name,
    arguments=args,
    namespace=namespace,
    spark_class=main_class,
    spark_jar=jar,
    spark_config="xsmall-etl",
    dag=dag
)

def format_slack_message(**kwargs):
    task_instance = kwargs["ti"]
    diff_result = task_instance.xcom_pull(task_ids="check_schema_difference", key="return_value")
    message = """
    :large_orange_circle: Missing Tables in Centro.\n
    """
    return message + "\n".join(str(x) for x in diff_result)

send_to_slack = SlackWebhookOperator(
    task_id="send_to_slack",
    http_conn_id=slack.base_url,
    webhook_token=slack.slack_hook_url,
    message=format_slack_message,
    dag=dag
)

check_schema_difference >> send_to_slack