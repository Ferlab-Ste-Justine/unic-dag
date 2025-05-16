"""
Update Centro schema
"""
# pylint: disable=missing-function-docstring, expression-not-assigned
from datetime import datetime, timedelta
from typing import List

from airflow import DAG
from airflow.models import Param

from lib.config import DEFAULT_ARGS, DEFAULT_TIMEOUT_HOURS, SPARK_FAILURE_MSG
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.notify import end, start

JAR = 's3a://spark-prd/jars/unic-etl-master.jar'
ZONE = 'red'
MAIN_CLASS = 'bio.ferlab.ui.etl.schema.UpdateSchema'
DOC = """
# Update Schema DAG

ETL pour la mise à jour de schémas.

### Description
Cet ETL lance un script de mise à jour de schémas se trouvant dans unic-etl pour les datasets correspondant au regex
passé en entrée.
"""


def get_dataset() -> str:
    return "{{ params.dataset }}"


def get_cluster_type() -> str:
    return "{{ params.cluster_type }}"


dag = DAG(
    dag_id="update_schema",
    params={
        "branch": Param("master", type="string"),
        "dataset": Param("", type="string"),
        "cluster_type": Param("small", type="string")
    },
    doc_md=DOC,
    start_date=datetime(2024, 12, 13),
    schedule_interval=None,
    dagrun_timeout=timedelta(hours=DEFAULT_TIMEOUT_HOURS),
    default_args=DEFAULT_ARGS,
    is_paused_upon_creation=True,
    max_active_tasks=3,
    tags=["schema"],
    on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
)

with dag:
    def arguments(dataset_regex: str) -> List[str]:
        return ["config/prod.conf", "default", dataset_regex]


    update_schema = SparkOperator(
        task_id="update_schema",
        name="update-schema",
        arguments=["config/prod.conf", "default", get_dataset()],
        zone=ZONE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=SPARK_FAILURE_MSG,
        spark_config=f"{get_cluster_type()}-etl",
        dag=dag
    )

    start() >> update_schema >> end()
