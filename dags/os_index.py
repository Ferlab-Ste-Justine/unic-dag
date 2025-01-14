"""
Génération des DAGs pour le load des indexs OpenSearch.
Un DAG par environnement postgres est généré.
"""
# pylint: disable=missing-function-docstring, invalid-name, expression-not-assigned, cell-var-from-loop, syntax-error, redefined-outer-name, duplicate-code

from datetime import datetime
from typing import List

from airflow import DAG
from airflow.decorators import task_group
from airflow.models import Param
from airflow.utils.trigger_rule import TriggerRule

from lib.postgres import PostgresEnv
from lib.opensearch import OpensearchEnv, os_port, os_qa_url, os_prod_url
from lib.config import jar, spark_failure_msg
# from lib.slack import Slack
from lib.tasks.notify import start, end
from lib.tasks.opensearch import load_index, get_release_id, publish_index

os_env_name = None
pg_env_name = None

env_dict = {
    OpensearchEnv.QA : PostgresEnv.DEV,
    OpensearchEnv.PROD : PostgresEnv.PROD
}

def load_index_arguments(release_id: str, template_filename: str, alias: str) -> List[str]:
    url = os_prod_url if pg_env_name == OpensearchEnv.PROD else os_qa_url

    return [
        "--env", pg_env_name,
        "--osurl", url,
        "--osport", os_port,
        "--osenv", os_env_name,
        "--release-id", release_id,
        "--template-filename", template_filename,
        "--alias", alias,
        "--config", "config/prod.conf"
    ]

def publish_index_arguments(release_id: str, alias: str) -> List[str]:
    url = os_prod_url if pg_env_name == OpensearchEnv.PROD else os_qa_url

    return [
        "--osurl", url,
        "--osport", os_port,
        "--release-id", release_id,
        "--alias", alias
    ]

def release_id() -> str:
    return '{{ params.release_id or "" }}'

# Update default args
args = {
    "owner": "unic",
    "depends_on_past": False,
    'trigger_rule': TriggerRule.NONE_FAILED
}

for os_env in OpensearchEnv:

    os_env_name = os_env.value
    pg_env_name = env_dict[os_env]

    doc = f"""
    # Load {pg_env_name} Index into OpenSeach {os_env_name} 
    
    DAG pour le load des Index {pg_env_name} dans OpenSearch {os_env_name}.
    
    ### Description
    Ce DAG load les index {pg_env_name} dans OpenSearch {os_env_name}.
    
    ## Indexs à Loader
    * resource centric
    * table centric 
    * variable centric 
    """

    with DAG(
            dag_id=f"os_{os_env_name}_index",
            params={
                "branch": Param("master", type="string"),
                "release_id": Param("", type=["null", "string"])
            },
            default_args=args,
            doc_md=doc,
            start_date=datetime(2024, 11, 19),
            is_paused_upon_creation=False,
            schedule_interval=None,
            tags=["opensearch"]
            # on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
    ) as dag:
        @task_group(group_id="load_indexes")
        def load_index_group(release_id: str):
            es_load_index_conf = [
                ("os_index_resource_centric", "resource_centric", "large-etl", "resource_centric_template.json"),
                ("os_index_table_centric", "table_centric", "large-etl", "table_centric_template.json"),
                ("os_index_variable_centric", "variable_centric", "large-etl", "variable_centric_template.json")
            ]

            [load_index(task_id, load_index_arguments(release_id, template_filename, alias),
                        jar, spark_failure_msg, cluster_size, dag) for
             task_id, alias, cluster_size, template_filename in es_load_index_conf]


        @task_group(group_id="publish_indexes")
        def publish_index_group(release_id: str):
            os_publish_index_conf = [
                ("os_publish_index_resource_centric", "resource_centric", "large-etl"),
                ("os_publish_index_table_centric", "table_centric", "large-etl"),
                ("os_publish_index_variable_centric", "variable_centric", "large-etl")
            ]

            [publish_index(task_id, publish_index_arguments(release_id, alias),
                           jar, spark_failure_msg, cluster_size, os_env, dag) for
             task_id, alias, cluster_size in os_publish_index_conf]


        get_release_id_task = get_release_id(release_id(),
                                             "resource_centric")  # the release id will be the same for all indexes

        start("start_os_index") >> get_release_id_task >> load_index_group(release_id=get_release_id_task) \
        >> publish_index_group(release_id=get_release_id_task) >> end("end_os_index")
