"""
DAG pour l'optimization des tables deltas
"""
# pylint: disable=missing-function-docstring, invalid-name, expression-not-assigned

from typing import List

from airflow import DAG
from airflow.models import Param
from airflow.utils.trigger_rule import TriggerRule

from lib.config import jar, spark_failure_msg
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.notify import start, end

DOC = """
# Optimization des Tables Deltas

DAG pour optimizer les tables deltas dans Minio

### Description
Ce DAG prends une liste de dataset ids et compacte et vacuum l'entièreté des données pour ces tables. 

### Configuration
* Paramètre `branch` : Branche du jar à utiliser.
* Paramètre `dataset ids` : Liste des tables à optimizer. Les tables doivent tous être dans la même zone.
* Paramètre `number of versions` : Nombre de version de la table à garder. (nombre de versions)
* Paramètre `zone` : Zone ou appliquer (nombre de versions)

"""

MAIN_CLASS = "bio.ferlab.datalake.spark3.utils.OptimizeDeltaTables.Main"

with DAG(
        dag_id="optimize_delta_tables",
        params={
            "branch": Param("master", type="string"),
            "dataset-ids": Param([], type=["array"],description="Tables to optimize."), # add examples of all tables?
            "number-of-versions": Param(10, type="integer", description="Number of versions to keep during vacuum"), # what should the default be?
            "zone": Param("red", type="string", enum=["red", "yellow", "green"])
        },
        default_args={
            'trigger_rule': TriggerRule.NONE_FAILED,
            'on_failure_callback': Slack.notify_task_failure,
        },
        doc_md=DOC,
        render_template_as_native_obj=True,
        is_paused_upon_creation=True,
        schedule_interval=None,

) as dag:

    def get_dataset_ids() -> List[str]:
        return "{{ params.number-of-versions or '' }}".split(",") # this may fail, try dagRun if case

    def get_number_of_versions() -> int:
        return int("{{ params.number-of-versions or '' }}") # this may fail, try dagRun if case

    def get_zone() -> str:
        return "{{ params.zone or '' }}"

    def arguments(dataset_ids: list, number_of_versions: int, app_name: str) -> List[str]:
        args = [
            dataset_ids,
            number_of_versions,
            "--config", "config/prod.conf",
            "--steps", "default",
            "--app-name", app_name
        ]

        return args

    optimize_tables = SparkOperator(
        task_id="optimize_delta_tables",
        name="optimize-delta-tables",
        arguments=arguments(dataset_ids=get_dataset_ids(), number_of_versions=get_number_of_versions(), app_name="optimize_delta_tables"),
        zone=get_zone(),
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="medium-etl",
        dag=dag
    )

    start("start_optimize_delta_tables") >> optimize_tables >> end("end_optimize_delta_tables")
