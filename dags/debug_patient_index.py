"""
Debug Patient Index DAG
"""
# pylint: disable=missing-function-docstring, duplicate-code, expression-not-assigned
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Param

from lib.config import DEFAULT_ARGS, SPARK_FAILURE_MSG, CONFIG_FILE
from lib.operators.spark import SparkOperator
from lib.tasks.notify import end, start

JAR = 's3a://spark-prd/jars/unic-etl-{{ params.branch }}.jar'

DOC = """
# Patient Index Debug DAG

### Description
Dag pour debugger le patient index. Ecrit dans s3a://red-test/curated/unic/patient_index.
"""

# Update default args
args = DEFAULT_ARGS.copy()
args.update({'trigger_rule': TriggerRule.NONE_FAILED})

params = {
    "branch": Param("UNIC-PATIENT-INDEX", type="string"),
    "version": Param("latest", type="string")
}

dag = DAG(
    dag_id="debug_patient_index",
    schedule_interval=None,
    params=params,
    default_args=args,
    start_date=datetime(2025, 2, 1, tzinfo=pendulum.timezone("America/Montreal")),
    concurrency=1,
    catchup=False,
    tags=["curated"],
    dagrun_timeout=timedelta(hours=1),
    is_paused_upon_creation=True
)

with dag:
    CURATED_ZONE = "red"
    CURATED_MAIN_CLASS = "bio.ferlab.ui.etl.red.curated.Main"

    args = [
        "curated_unic_patient_index",
        "--config", CONFIG_FILE,
        "--steps", "initial",
        "--app-name", "curated_unic_patient_index"
    ]

    test_curated_unic_patient_index = SparkOperator(
        task_id="test_curated_unic_patient_index",
        name="test-curated-unic-patient-index",
        arguments=args,
        zone=CURATED_ZONE,
        spark_class=CURATED_MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=SPARK_FAILURE_MSG,
        spark_config="large-etl",
        dag=dag
    )

    start("start_debug_patient_index") >> test_curated_unic_patient_index >> end("debug_patient_index")
