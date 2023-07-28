from airflow.models import Variable, Param

from core.failure import Failure

def generate_default_args(owner, on_failure_callback):
    return {
        "owner": owner,
        "depends_on_past": False,
        "on_failure_callback": on_failure_callback
    }


root = Variable.get('dags_config', '/opt/airflow/dags/repo/dags/config')
extract_schema = '(.*)_config.json'
config_file = "config/prod.conf"

default_args = generate_default_args(owner="unic", on_failure_callback=Failure.on_failure_callback)
default_params = {
    "branch": Param("master", type="string"),
    "version": Param("latest", type="string")
}

spark_failure_msg = "Spark job failed"
default_timeout_hours = 4

jar = 's3a://spark-prd/jars/unic-etl-{{ params.branch }}.jar'
version = '{{ params.version }}'

mail_from = Variable.get("AIRFLOW__SMTP__SMTP_MAIL_FROM")

