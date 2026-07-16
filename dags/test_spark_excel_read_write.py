"""
Test spark-excel package DAG
"""
from datetime import datetime, timedelta
from typing import List

from airflow import DAG

from lib.config import DEFAULT_PARAMS, DEFAULT_TIMEOUT_HOURS, DEFAULT_ARGS, SPARK_FAILURE_MSG, JAR, \
    CONFIG_FILE, LOCAL_TZ
from lib.operators.spark import SparkOperator
from lib.tasks.notify import end, start

DOC = """
# Test spark-excel package DAG

ETL pour tester la lecture et écriture des fichiers Excel.
"""

# Update default args
args = DEFAULT_ARGS.copy()

dag = DAG(
    dag_id="test_spark_excel_read_write",
    doc_md=DOC,
    start_date=datetime(2023, 10, 20, 7, tzinfo=LOCAL_TZ),
    schedule_interval=None,
    params=DEFAULT_PARAMS,
    dagrun_timeout=timedelta(hours=DEFAULT_TIMEOUT_HOURS),
    default_args=args,
    is_paused_upon_creation=True,
)

with dag:
    ZONE = "yellow"
    MAIN_CLASS = "bio.ferlab.ui.etl.qa.ExcelReadWriteETL"


    def arguments() -> List[str]:
        """
        Constructs cli arguments for etl

        :return: A list of strings representing the constructed command-line arguments.
        :rtype: List[str]
        """
        return [
            "--config", CONFIG_FILE,
            "--steps", "default",
            "--app-name", "test_spark_excel_read_write",
        ]


    perform_test = SparkOperator(
        task_id="unic-test-excel-read-write",
        name="unic-test-excel-read-write",
        arguments=arguments(),
        zone=ZONE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=SPARK_FAILURE_MSG,
        spark_config="small-etl",
        dag=dag
    )

    start() >> perform_test >> end()
