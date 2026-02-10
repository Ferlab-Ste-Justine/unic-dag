"""
Test spark-excel package DAG
"""

from datetime import datetime, timedelta
from typing import List

import pendulum
from airflow import DAG

from lib.config import DEFAULT_PARAMS, DEFAULT_TIMEOUT_HOURS, DEFAULT_ARGS, SPARK_FAILURE_MSG
from lib.operators.spark import SparkOperator
from lib.tasks.notify import end, start

JAR = 's3a://spark-prd/jars/unic-etl-{{ params.branch }}.jar'

DOC = """
# Test spark-excel package DAG

ETL pour tester la lecture et écriture des fichiers Excel. 
"""

# Update default args
args = DEFAULT_ARGS.copy()

dag = DAG(
    dag_id="test_spark_excel_read_write",
    doc_md=DOC,
    start_date=datetime(2023, 10, 20, 7, tzinfo=pendulum.timezone("America/Montreal")),
    schedule_interval=None,
    params=DEFAULT_PARAMS,
    dagrun_timeout=timedelta(hours=DEFAULT_TIMEOUT_HOURS),
    default_args=args,
    is_paused_upon_creation=True,
)

with dag:
    ZONE = "yellow"
    MAIN_CLASS = "bio.ferlab.ui.etl.qa.ExcelReadWriteETL"


    def arguments(entrypoint: str) -> List[str]:
        return [
            entrypoint,
            "--config", "config/prod.conf",
            "--steps", "default",
            "--app-name", entrypoint,
        ]


    perform_test = SparkOperator(
        task_id="unic-test-excel-read-write",
        name="unic-test-excel-read-write",
        arguments=arguments("perform_read_write"),
        zone=ZONE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=SPARK_FAILURE_MSG,
        spark_config="small-etl",
        dag=dag
    )

    start() >> perform_test >> end()
