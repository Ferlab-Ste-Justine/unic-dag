"""
Génération des DAGs pour le publication du d'un projet de recherche dans le Portail de l'UNIC.
Un DAG par environnement postgres est généré.
"""
# pylint: disable=missing-function-docstring, invalid-name, expression-not-assigned, raise-missing-from, cell-var-from-loop
from datetime import datetime
from typing import List

from airflow import DAG
from airflow.models import Param
from airflow.utils.trigger_rule import TriggerRule

from lib.groups.publish.index import index_opensearch
from lib.groups.publish.publish import publish_research_project
from lib.tasks.notify import start, end
from lib.opensearch import pg_env_os_env_mapping
from lib.config import DEFAULT_ARGS
from lib.postgres import PostgresEnv, unic_postgres_vlan2_conn_id
from lib.slack import Slack

# Update default args
dag_args = DEFAULT_ARGS.copy()
dag_args.update({
    'trigger_rule': TriggerRule.NONE_FAILED,
    'on_failure_callback': Slack.notify_task_failure})

for env in PostgresEnv:
    env_name = env.value
    pg_conn_id = unic_postgres_vlan2_conn_id(env)

    doc = f"""
    # Publish Reasearch Project
    
    DAG pour la publication d'un projet de recherche dans le portail de l'UNIC dans l'environnement **{env_name}**.
    
    ### Description
    Ce DAG publie le dictionaire et les données d'un projet dans la zone published de minio et inclue le projet dans les indexs
    opensearch du portail UNIC. Il a un dag par environnment postgres.
    
    ### Configuration
    * Paramètre `resource_code` : Code de la ressource à charger.
    * Paramètre `version_to_publish` : date version dans released à publier dans published.
    * Paramètre `release_id` (optional) : release id of opensearch index.

    """

    with DAG(
            dag_id=f"unic_publish_project_{env_name}",
            params={
                "resource_code": Param("", type="string", description="Resource to publish. Ex: cpip"),
                "version_to_publish": Param("", type="string", description="Date to publish. Ex: 2001-01-01"),
                "release_id": Param("", type=["null", "string"], description="(Optional) Release id of OpenSearch index. If not specified, will increment. Ex: re_0000")
            },
            default_args=dag_args,
            doc_md=doc,
            start_date=datetime(2024, 12, 16),
            is_paused_upon_creation=False,
            render_template_as_native_obj=True,
            schedule_interval=None,
            tags=["postgresql", "published", "opensearch"],
            on_failure_callback=Slack.notify_dag_failure
    ) as dag:

        start("start_unic_publish_project") \
        >> publish_research_project(pg_conn_id) \
        >> index_opensearch(env_name, pg_env_os_env_mapping.get(env).value, dag) \
        >> end("end_unic_publish_project")