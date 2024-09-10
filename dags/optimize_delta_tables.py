"""
DAG pour l'optimization des tables deltas
"""
# pylint: disable=missing-function-docstring, invalid-name, expression-not-assigned, too-many-ancestors

from datetime import datetime

from typing import List

from airflow.decorators import task
from airflow import DAG
from airflow.models import Param, DagRun
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
            "dataset_ids": Param([], type=["array"],description="Tables to optimize."),
            "number_of_versions": Param("10", type="string", description="Number of versions to keep during vacuum"),
            "zone": Param("yellow", type="string", enum=["red", "yellow", "green"])
        },
        default_args={
            'trigger_rule': TriggerRule.NONE_FAILED,
            'on_failure_callback': Slack.notify_task_failure,
        },
        doc_md=DOC,
        start_date=datetime(2024, 9, 9),
        render_template_as_native_obj=True,
        is_paused_upon_creation=True,
        schedule_interval=None,

) as dag:

    def arguments(app_name: str) -> List[str]:
        args = [
            "--config", "config/prod.conf",
            "--steps", "default",
            "--app-name", app_name
        ]

        return args

    def get_zone() -> str:
        return "{{ params.zone }}"

    def get_number_of_versions() -> str:
        return "{{ params.number_of_versions }}"

    @task
    def get_dataset_ids(ti=None) -> List[str]:
        dag_run: DagRun = ti.dag_run

        dataset_ids_args = []
        dataset_ids = dag_run.conf['dataset_ids']
        [dataset_ids_args.extend(['--dataset_id', d]) for d in dataset_ids]
        return dataset_ids_args


    class OptimizeDeltaTables(SparkOperator):
        """Custom Operator for Delta Table Optimization"""
        template_fields = SparkOperator.template_fields + ('arguments', 'dataset_ids', 'number_of_versions')

        def __init__(self,
                     dataset_ids,
                     number_of_versions,
                     **kwargs):
            super().__init__(**kwargs)
            self.dataset_ids = dataset_ids
            self.number_of_versions = number_of_versions

        def execute(self, **kwargs):
            # Append dataset_ids to arguments at runtime, after dataset_ids has been templated. Otherwise, dataset_ids
            # is interpreted as XComArg and can't be appended to arguments.
            self.arguments = self.arguments + self.dataset_ids + "--number-of-versions" + str(self.number_of_versions)
            super().execute(**kwargs)

    def optimize_delta_tables(dataset_ids: List[str]):
        return OptimizeDeltaTables(
            task_id="optimize_delta_tables",
            name="optimize-delta-tables",
            arguments=arguments(app_name="optimize_delta_tables"),
            dataset_ids=dataset_ids,
            number_of_versions = get_number_of_versions(),
            zone=get_zone(),
            spark_class=MAIN_CLASS,
            spark_jar=jar,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

    get_dataset_ids_task = get_dataset_ids()

    start("start_optimize_delta_tables") >> get_dataset_ids_task \
    >> optimize_delta_tables(dataset_ids=get_dataset_ids_task) \
    >> end("end_optimize_delta_tables")
