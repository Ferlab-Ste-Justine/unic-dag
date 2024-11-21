import os
from datetime import timedelta

from airflow.models import Variable, Param

from lib.failure import Failure
from lib.slack import Slack


def generate_default_args(owner, on_failure_callback, on_retry_callback):
    return {
        "owner": owner,
        "depends_on_past": False,
        "on_failure_callback": on_failure_callback,
        "on_retry_callback": on_retry_callback,
        "retries": 3,
        "retry_delay": timedelta(minutes=3),
    }


root = Variable.get('dags_path', '/opt/airflow/dags/repo/dags')
dags_config_path = f"{root}/config"
extract_schema = '(.*)_config.json'
config_file = "config/prod.conf"

default_args = generate_default_args(owner="unic",
                                     on_failure_callback=Failure.task_on_failure_callback,
                                     on_retry_callback=Slack.notify_task_retry)
default_params = {
    "branch": Param("master", type="string"),
    "version": Param("latest", type="string")
}

spark_failure_msg = "Spark job failed"
spark_test_failure_msg = "Spark test job failed"
default_timeout_hours = 4

jar = 's3a://spark-prd/jars/unic-etl-{{ params.branch }}.jar'
version = '{{ params.version }}'

minio_conn_id = "minio"
yellow_minio_conn_id = "yellow_minio"
green_minio_conn_id = "green_minio"
mail_from = os.environ.get("AIRFLOW__SMTP__SMTP_MAIL_FROM")

es_url = 'http://unic-prod-opensearch-workers:9200'

