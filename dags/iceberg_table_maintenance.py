"""
Iceberg Table Maintenance DAG
"""
# pylint: disable=missing-function-docstring, duplicate-code, expression-not-assigned
from datetime import datetime, timedelta
from typing import List

import pendulum
from airflow import DAG
from airflow.models import Param

from lib.config import DEFAULT_TIMEOUT_HOURS, DEFAULT_ARGS, SPARK_FAILURE_MSG, JAR
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.notify import end, start

DOC = """
# Iceberg Table Maintenance DAG

ETL pour la maintenance des tables Iceberg.

### Description
Ce DAG est utilisé pour effectuer la maintenance des tables Iceberg, notamment la supression des tables orphelines dans le catalog iceberg spécifié. 
Il roule chaque jour a 23h00.

"""
ZONE = "red"
MAIN_CLASS = "bio.ferlab.ui.etl.optimization.IcebergTableMaintenance.Main"

dag = DAG(
    dag_id="iceberg_table_maintenance",
    doc_md=DOC,
    start_date=datetime(2025, 6, 10, 23, tzinfo=pendulum.timezone("America/Montreal")),
    schedule_interval=timedelta(days=1),
    params={
        "branch": Param("master", type="string"),
        "catalog": Param("cdc", type="string")
    },
    dagrun_timeout=timedelta(hours=DEFAULT_TIMEOUT_HOURS),
    default_args=DEFAULT_ARGS,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=1,
    max_active_tasks=2,
    tags=["raw"],
    on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
)

with dag:
    def get_catalog() -> str:
        return '{{ params.catalog or "" }}'

    def arguments(catalog: str) -> List[str]:
        return [
            "--config", "config/iceberg.conf",
            "--steps", "initial",
            "--app-name", f"iceberg_table_maintenance_{catalog}",
            "--catalog", catalog,
        ]

    iceberg_table_maintenance = SparkOperator(
        task_id="iceberg_table_maintenance",
        name="iceberg-table-maintenance",
        arguments=arguments(get_catalog()),
        zone=ZONE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=SPARK_FAILURE_MSG,
        spark_config="small-etl",
        dag=dag
    )

    start() >> iceberg_table_maintenance >> end()