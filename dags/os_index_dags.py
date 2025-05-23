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

from lib.opensearch import OpensearchEnv, OpensearchAlias
from lib.postgres import PostgresEnv
from lib.slack import Slack
from lib.tasks.notify import start, end
from lib.tasks.opensearch import load_index, publish_index, get_next_release_id


def load_index_arguments(release_id: str, os_url: str, os_port: str, template_filename: str, os_env_name: str, pg_env_name: str,
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

def get_release_id() -> str:
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
            tags=["opensearch"],
            on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
    ) as dag:
        @task_group(group_id="load_indexes")
        def load_index_group(release_id: str):
            for alias in OpensearchAlias:
                load_index.override(task_id=f"load_index_{alias.value}")(os_env_name, release_id, alias.value)

        @task_group(group_id="publish_indexes")
        def publish_index_group(release_id: str):
            for alias in OpensearchAlias:
                publish_index.override(task_id=f"publish_index_{alias.value}")(os_env_name, release_id, alias.value)

        get_next_release_id_task = get_next_release_id(os_env_name, get_release_id())

        start("start_os_index") >> get_next_release_id_task >> load_index_group(release_id=get_next_release_id_task) \
        >> publish_index_group(release_id=get_next_release_id_task) >> end("end_os_index")
