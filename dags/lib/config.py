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
extract_resource = '(.*)_config.json'
config_file = "config/prod.conf"

default_args = generate_default_args(owner="unic",
                                     on_failure_callback=Failure.task_on_failure_callback,
                                     on_retry_callback=Slack.notify_task_retry)
default_params = {
    "branch": Param("master", type="string"),
    "version": Param("latest", type="string")
}

spark_failure_msg = "Spark job failed"
default_timeout_hours = 4

jar = 's3a://spark-prd/jars/unic-etl-{{ params.branch }}.jar'
version = '{{ params.version }}'

minio_conn_id = "minio"
yellow_minio_conn_id = "yellow_minio"
green_minio_conn_id = "green_minio"
mail_from = os.environ.get("AIRFLOW__SMTP__SMTP_MAIL_FROM")

# Opensearch prod configs
os_url = 'https://workers.opensearch.unic.sainte-justine.intranet'
os_port = '9200'
os_credentials ='opensearch-dags-credentials',
os_username='OS_USERNAME',
os_password='OS_PASSWORD',
os_cert='unic-prod-opensearch-ca-certificate',

# Opensearch qa congigs
os_qa_url = 'https://workers.opensearch.qa.unic.sainte-justine.intranet'
os_qa_credentials ='opensearch-qa-dags-credentials',
os_qa_username='OS_QA_USERNAME',
os_qa_password='OS_QA_PASSWORD',
os_qa_cert='unic-prod-opensearch-qa-ca-certificate',

# QA tests config
qa_test_main_class = "bio.ferlab.ui.etl.qa.Main"
qa_test_cluster_type = "xsmall"
qa_test_spark_failure_msg = "Spark test job failed"
qa_test_retries = 0

# Optimization config
optimization_main_class = "bio.ferlab.ui.etl.optimization.DeltaTableOptimization"
optimization_cluster_type = "small"
optimization_spark_failure_msg = "Spark optimization job failed"
optimization_retries = 0
