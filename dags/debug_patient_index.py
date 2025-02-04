"""
Debug Patient Index DAG
"""
# pylint: disable=missing-function-docstring, duplicate-code, expression-not-assigned
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule

from lib.config import default_params, default_args, spark_failure_msg, config_file
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.notify import end, start

JAR = 's3a://spark-prd/jars/unic-etl-UNIC-PATIENTINDEX.jar'

DOC = """
# Patient Index Debug DAG

### Description
Dag pour debugger le patient index. Ecrit dans s3a://red-test/curated/unic/patient_index.
"""

# Update default args
args = default_args.copy()
args.update({'trigger_rule': TriggerRule.NONE_FAILED})

dag = DAG(
    dag_id="debug_patient_index",
    schedule_interval=None,
    params=default_params,
    default_args=args,
    start_date=datetime(2025, 2, 1, tzinfo=pendulum.timezone("America/Montreal")),
    concurrency=1,
    catchup=False,
    tags=["curated"],
    dagrun_timeout=timedelta(hours=1),
    is_paused_upon_creation=True,
    on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
)

with dag:
    CURATED_ZONE = "red"
    CURATED_MAIN_CLASS = "bio.ferlab.ui.etl.red.curated.Main"

    args = [
        "curated_unic_patient_index",
        "--config", config_file,
        "--steps", "default",
        "--app-name", "curated_unic_patient_index"
    ]

    test_curated_unic_patient_index = SparkOperator(
        task_id="test_curated_unic_patient_index",
        name="test-curated-unic-patient-index",
        arguments=args,
        zone=CURATED_ZONE,
        spark_class=CURATED_MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=spark_failure_msg,
        spark_config="large-etl",
        dag=dag
    )

    start("start_debug_patient_index") >> test_curated_unic_patient_index >> end("debug_patient_index")
