"""
Iceberg Table Maintenance DAG
"""
from datetime import timedelta
from typing import List

import pendulum
from airflow import DAG
from airflow.models import Param

from lib.operators.spark_iceberg import SparkIcebergOperator
from lib.config import DEFAULT_TIMEOUT_HOURS, DEFAULT_ARGS, SPARK_FAILURE_MSG, JAR, LOCAL_TZ
from lib.slack import Slack
from lib.tasks.notify import end, start
from timetables import IntervalTimetable

DOC = """
# Iceberg Table Maintenance DAG

ETL pour la maintenance des tables Iceberg.

### Description
Ce DAG est utilisé pour effectuer la maintenance des tables Iceberg, notamment l'expiration des snapshots plus vieux que 1 jour 
et la supression des tables orphelines dans le catalog Iceberg spécifié. 
Il roule chaque jour a 23h00.

"""
ZONE = "red"
MAIN_CLASS = "bio.ferlab.ui.etl.optimization.iceberg.IcebergTableMaintenance"
EXPIRE_SNAPSHOTS_MAIN = "expireSnapshotsInCatalog"
DELETE_ORPHAN_FILES_MAIN = "deleteOrphanFilesInCatalog"

dag = DAG(
    dag_id="iceberg_table_maintenance",
    doc_md=DOC,
    start_date=pendulum.datetime(2025, 7, 29, 23, tz=LOCAL_TZ),
    schedule=IntervalTimetable(interval=timedelta(days=1)),
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

    def arguments(main: str, catalog: str) -> List[str]:
        return [
            main,
            "--config", "config/iceberg.conf",
            "--steps", "initial",
            "--app-name", f"iceberg_table_maintenance_{catalog}",
            "--iceberg-catalog", catalog,
        ]

    expire_snapshots = SparkIcebergOperator(
        task_id="expire_snapshots",
        name="expire-snapshots",
        arguments=arguments(EXPIRE_SNAPSHOTS_MAIN, get_catalog()),
        zone=ZONE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=SPARK_FAILURE_MSG,
        spark_config="small-etl",
        dag=dag
    )

    delete_orphan_files = SparkIcebergOperator(
        task_id="delete_orphan_files",
        name="delete-orphan-files",
        arguments=arguments(DELETE_ORPHAN_FILES_MAIN, get_catalog()),
        zone=ZONE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=SPARK_FAILURE_MSG,
        spark_config="small-etl",
        dag=dag
    )

    start() >> expire_snapshots >> delete_orphan_files >> end()
