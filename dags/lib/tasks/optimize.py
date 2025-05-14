from typing import List

from airflow import DAG

from lib.config import OPTIMIZATION_MAIN_CLASS, OPTIMIZATION_CLUSTER_TYPE, OPTIMIZATION_SPARK_FAILURE_MSG, \
    OPTIMIZATION_RETRIES
from lib.operators.spark import SparkOperator

def optimize(destinations: List[str], resource: str, zone: str, subzone: str,
         config_file: str, jar: str, dag: DAG) -> SparkOperator:
    """
    Create optimize task.

    :param destinations: List of destinations to optimize.
    :param resource: Resource name, e.g.'softpath'.
    :param zone: Zone of the destinations optimized: 'red', 'yellow' or 'green'.
    :param subzone: Subzone of the destinations optimized, e.g. 'anonymized'.
    :param config_file: Path of the ETL configuration file.
    :param jar: Path of the Spark jar.
    :param dag: Reference to the DAG.
    :return: SparkOperator task instance of the optimization.
    """
    task_id = "_".join([subzone, resource, "optimization"])

    args = [
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
        spark_class=OPTIMIZATION_MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=OPTIMIZATION_SPARK_FAILURE_MSG,
        spark_config=f"{OPTIMIZATION_CLUSTER_TYPE}-etl",
        retries=OPTIMIZATION_RETRIES,
        dag=dag
    )
