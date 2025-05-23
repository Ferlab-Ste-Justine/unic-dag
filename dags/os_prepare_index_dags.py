"""
Génération des DAGs pour le préaration des indexs OpenSearch.
Un DAG par environnement postgres est généré.
"""
from datetime import datetime
from typing import List

from airflow import DAG
from airflow.decorators import task_group
from airflow.utils.trigger_rule import TriggerRule

from lib.config import DEFAULT_PARAMS, JAR, SPARK_FAILURE_MSG, DEFAULT_ARGS
from lib.postgres import PostgresEnv
from lib.slack import Slack
from lib.tasks.notify import start, end
from lib.tasks.opensearch import prepare_index

# pylint: disable=missing-function-docstring, invalid-name, expression-not-assigned, cell-var-from-loop, duplicate-code, redefined-outer-name

# Update default args
args = DEFAULT_ARGS.copy()
args.update({
    'trigger_rule': TriggerRule.NONE_FAILED,
    'on_failure_callback': Slack.notify_task_failure})


def arguments(task_id: str, pg_env_name : str) -> List[str]:
    return [
        task_id,
        "--config", "config/prod.conf",
        "--steps", "initial",
        "--app-name", f"prepare_{task_id}",
        "--env", pg_env_name
    ]


for pg_env in PostgresEnv:
    pg_env_name = pg_env.value

    doc = f"""
    # Prepare OpenSearch **{pg_env_name}** Index 
    
    DAG pour la préparation des Index **{pg_env_name}** OpenSearch dans l'environnement.
    
    ### Description
    Ce DAG prepare les index **{pg_env_name}** OpenSearch pour ensuite être implementé dans OpenSearch.
    
    ## Indexs à Préparer
    * resource centric
    * table centric 
    * variable centric 
    """

    with DAG(
            dag_id=f"os_{pg_env_name}_prepare_index",
            params=DEFAULT_PARAMS,
            default_args=args,
            doc_md=doc,
            start_date=datetime(2024, 11, 18),
            is_paused_upon_creation=False,
            schedule_interval=None,
            tags=["opensearch"],
            on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
    ) as dag:
        @task_group(group_id="prepare_indexes")
        def prepare_index_group():
            os_prepare_index_conf = [
                ("os_index_resource_centric", "large-etl"),
                ("os_index_table_centric", "large-etl"),
                ("os_index_variable_centric", "large-etl")
            ]

            [prepare_index(task_id, arguments(task_id, pg_env_name), JAR, SPARK_FAILURE_MSG,
                           cluster_size, dag) for task_id, cluster_size in os_prepare_index_conf]


        start("start_os_prepare_index") >> prepare_index_group() >> end("end_os_prepare_index")
