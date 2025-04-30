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

from lib.config import jar, spark_failure_msg
from lib.opensearch import OpensearchEnv, os_port, os_qa_url, os_prod_url
from lib.postgres import PostgresEnv
# from lib.slack import Slack
from lib.tasks.notify import start, end
from lib.tasks.opensearch import load_index, publish_index, get_release_id


def load_index_arguments(release_id: str, os_url: str, template_filename: str, os_env_name: str, pg_env_name: str,
                         alias: str) -> List[str]:
    return [
        "--env", pg_env_name,
        "--osurl", os_url,
        "--osport", os_port,
        "--osenv", os_env_name,
        "--release-id", release_id,
        "--template-filename", template_filename,
        "--alias", alias,
        "--config", "config/prod.conf"
    ]


def publish_index_arguments(release_id: str, os_url: str, alias: str) -> List[str]:
    return [
        "--osurl", os_url,
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
    os_env_name: str = os_env.value
    pg_env_name: str = PostgresEnv.PROD.value  # Use PROD data for all environments
    # pg_env_name: str = os_env_pg_env_mapping[os_env].value

    doc = f"""
    # Load {pg_env_name} Index into OpenSeach {os_env_name} 
    
    DAG pour le load des index PROD dans OpenSearch {os_env_name}.
    
    ### Description
    Ce DAG load les index PROD dans OpenSearch {os_env_name}.
    
    ## Index à Loader
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
            os_url = os_prod_url if os_env_name == OpensearchEnv.PROD.value else os_qa_url

            es_load_index_conf = [
                ("os_index_resource_centric", "resource_centric", "large-etl", "resource_centric_template.json"),
                ("os_index_table_centric", "table_centric", "large-etl", "table_centric_template.json"),
                ("os_index_variable_centric", "variable_centric", "large-etl", "variable_centric_template.json")
            ]

            [load_index(task_id, load_index_arguments(release_id, os_url, template_filename, os_env_name, pg_env_name, alias),
                        jar, spark_failure_msg, cluster_size, dag) for
             task_id, alias, cluster_size, template_filename in es_load_index_conf]


        @task_group(group_id="publish_indexes")
        def publish_index_group(release_id: str):
            os_url = os_prod_url if os_env_name == OpensearchEnv.PROD.value else os_qa_url

            os_publish_index_conf = [
                ("os_publish_index_resource_centric", "resource_centric", "xsmall-etl"),
                ("os_publish_index_table_centric", "table_centric", "xsmall-etl"),
                ("os_publish_index_variable_centric", "variable_centric", "xsmall-etl")
            ]

            [publish_index(task_id, publish_index_arguments(release_id, os_url, alias),
                           jar, spark_failure_msg, cluster_size, os_env_name, dag) for
             task_id, alias, cluster_size in os_publish_index_conf]


        get_release_id_task = get_release_id("os_get_release_id", os_env_name, release_id())

        start("start_os_index") >> get_release_id_task >> load_index_group(release_id=get_release_id_task) \
        >> publish_index_group(release_id=get_release_id_task) >> end("end_os_index")
