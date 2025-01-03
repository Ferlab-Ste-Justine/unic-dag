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

from lib.config import jar, spark_failure_msg, os_url, os_port
from lib.postgres import PostgresEnv
# from lib.slack import Slack
from lib.tasks.notify import start, end
from lib.tasks.opensearch import load_index, get_release_id, publish_index

env_name = None


def load_index_arguments(release_id: str, template_filename: str, alias: str) -> List[str]:
    return [
        "--env", env_name,
        "--osurl", os_url,
        "--osport", os_port,
        "--release-id", release_id,
        "--template-filename", template_filename,
        "--alias", alias,
        "--config", "config/prod.conf"
    ]


def publish_index_arguments(release_id: str, alias: str) -> List[str]:
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

for env in PostgresEnv:
    env_name = env.value

    doc = f"""
    # Load OpenSearch Index **{env_name}**
    
    DAG pour le load des Index OpenSearch dans l'environnement **{env_name}**.
    
    ### Description
    Ce DAG load les index OpenSearch dans l'environnement **{env_name}**. Ceci va ecentuallement faire parti du dag de publication.
    Ce dag est pour tester cet étape.
    
    ## Indexs à Loader
    * resource centric
    * table centric 
    * variable centric 
    """

    with DAG(
            dag_id=f"es_{env_name}_index",
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
                ("es_index_resource_centric", "resource_centric", "large-etl", "resource_centric_template.json"),
                ("es_index_table_centric", "table_centric", "large-etl", "table_centric_template.json"),
                ("es_index_variable_centric", "variable_centric", "large-etl", "variable_centric_template.json")
            ]

            [load_index(task_id, load_index_arguments(release_id, template_filename, alias),
                        jar, spark_failure_msg, cluster_size, dag) for
             task_id, alias, cluster_size, template_filename in es_load_index_conf]


        @task_group(group_id="publish_indexes")
        def publish_index_group(release_id: str):
            es_publish_index_conf = [
                ("es_publish_index_resource_centric", "resource_centric", "large-etl"),
                ("es_publish_index_table_centric", "table_centric", "large-etl"),
                ("es_publish_index_variable_centric", "variable_centric", "large-etl")
            ]

            [publish_index(task_id, publish_index_arguments(release_id, alias),
                           jar, spark_failure_msg, cluster_size, dag) for
             task_id, alias, cluster_size in es_publish_index_conf]


        get_release_id_task = get_release_id(release_id(),
                                             "resource_centric")  # the release id will be the same for all indexes

        start("start_es_prepare_index") >> get_release_id_task >> load_index_group(release_id=get_release_id_task) \
        >> publish_index_group(release_id=get_release_id_task) >> end("end_postgres_prepare_index")
