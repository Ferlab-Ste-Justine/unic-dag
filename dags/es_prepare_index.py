"""
Génération des DAGs pour le préaration des indexs OpenSearch.
Un DAG par environnement postgres est généré.
"""
# pylint: disable=missing-function-docstring, invalid-name, expression-not-assigned, cell-var-from-loop

from datetime import datetime
from typing import List

from airflow import DAG
from airflow.decorators import task_group
from airflow.utils.trigger_rule import TriggerRule

from lib.tasks.opensearch import prepare_index
from lib.config import default_params, jar, spark_failure_msg
from lib.postgres import PostgresEnv
from lib.slack import Slack
from lib.tasks.notify import start, end

env_name = None

def arguments(task_id: str) -> List[str]:
    return [
        task_id,
        "--config", "config/prod.conf",
        "--steps", "default",
        "--app-name", f"prepare_{task_id}",
        "--env", env_name
    ]


for env in PostgresEnv:
    env_name = env.value

    doc = f"""
    # Prepare OpenSearch Index **{env_name}**
    
    DAG pour la préparation des Index OpenSearch dans l'environnement **{env_name}**.
    
    ### Description
    Ce DAG prepare les index OpenSearch dans l'environnement **{env_name}** pour ensuite être implementé dans OpenSearch.
    
    ## Indexs à Préparer
    * resource centric
    * table centric 
    * variable centric 
    """

    with DAG(
            dag_id=f"es_{env_name}_prepare_index",
            params=default_params,
            default_args={
                'trigger_rule': TriggerRule.NONE_FAILED,
                'on_failure_callback': Slack.notify_task_failure,
            },
            doc_md=doc,
            start_date=datetime(2024, 11, 18),
            is_paused_upon_creation=False,
            schedule_interval=None,
            tags=["opensearch"]
    ) as dag:

        @task_group(group_id="prepare_indexes")
        def prepare_index_group():
            es_prepare_index_conf = [
                ("es_index_resource_centric", "large-etl"),
                ("es_index_table_centric", "large-etl"),
                ("es_index_variable_centric", "large-etl")
            ]

            [prepare_index(task_id, arguments(task_id), jar, spark_failure_msg,
                           cluster_size, dag) for task_id, cluster_size in es_prepare_index_conf]


        start("start_es_prepare_index") >> prepare_index_group() >> end("end_postgres_prepare_index")
