from typing import List

from airflow import DAG

from lib.config import QA_TEST_MAIN_CLASS, QA_TEST_CLUSTER_TYPE, QA_TEST_SPARK_FAILURE_MSG, \
    QA_TEST_RETRIES
from lib.operators.spark import SparkOperator


def test(test_name: str, destinations: List[str], resource: str, zone: str, subzone: str,
         config_file: str, jar: str, dag: DAG, cluster_type : str = QA_TEST_CLUSTER_TYPE) -> SparkOperator:
    """
    Create QA test task.

    :param test_name: Name of the QA test and the entrypoint of the test in the Main class.
    :param destinations: List of destinations to test.
    :param resource: Resource name, e.g.'softpath'.
    :param zone: Zone of the destinations tested: 'red', 'yellow' or 'green'.
    :param subzone: Subzone of the destinations tested, e.g. 'anonymized'.
    :param config_file: Path of the ETL configuration file.
    :param jar: Path of the Spark jar.
    :param dag: Reference to the DAG.
    :return: SparkOperator task instance of the QA test.
    """
    task_id = "_".join([subzone, resource, test_name])

    args = [
        test_name,
        "--config", config_file,
        "--steps", "default",
        "--app-name", task_id,
        *[arg for dst in destinations for arg in ("--destination", dst)]
    ]

    return SparkOperator(
        task_id=task_id,
        name=task_id.replace("_", "-")[:40],
        zone=zone,
        arguments=args,
        spark_class=QA_TEST_MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=QA_TEST_SPARK_FAILURE_MSG,
        spark_config=f"{cluster_type}-etl",
        retries=QA_TEST_RETRIES,
        dag=dag
    )
