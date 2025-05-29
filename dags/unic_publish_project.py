"""
Génération des DAGs pour le publication du d'un projet de recherche dans le Portail de l'UNIC.
Un DAG par environnement postgres est généré.
"""
# pylint: disable=missing-function-docstring, invalid-name, expression-not-assigned, raise-missing-from, cell-var-from-loop, duplicate-code
from datetime import datetime

from airflow import DAG
from airflow.models import Param
from airflow.utils.trigger_rule import TriggerRule

from lib.groups.publish.index import index_opensearch
from lib.groups.publish.publish import publish_research_project
from lib.tasks.publish import validate_to_be_published, get_resource_code, get_version_to_publish, get_include_dictionary
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
                "include_dictionary": Param(True, type="boolean", description="Include dictionary in publication of project"),
                "release_id": Param("", type=["null", "string"], description="(Optional) Release id of OpenSearch index. If not specified, will increment. Ex: re_0000")
            },
            default_args=dag_args,
            doc_md=doc,
            start_date=datetime(2024, 12, 16),
            is_paused_upon_creation=False,
            render_template_as_native_obj=True,
            schedule_interval=None,
            tags=["postgresql", "published", "opensearch"],
            on_failure_callback=Slack.notify_dag_failure,
            # on_skipped_callback=Slack.notify_task_skip # Available airflow 2.9
    ) as dag:
        get_resource_code_task = get_resource_code()
        get_version_to_publish_task = get_version_to_publish()
        get_include_dictionary_task = get_include_dictionary()

        start("start_unic_publish_project") \
        >> [get_resource_code_task, get_version_to_publish_task, get_include_dictionary_task] \
        >> publish_research_project(
            pg_conn_id=pg_conn_id,
            resource_code=get_resource_code_task,
            version_to_publish=get_version_to_publish_task,
            include_dictionary=get_include_dictionary_task
        ) \
        >> validate_to_be_published(
            resource_code=get_resource_code_task,
            pg_conn_id=pg_conn_id
        ) \
        >> index_opensearch(
            pg_env_name=env_name,
            os_env_name=pg_env_os_env_mapping.get(env).value,
            dag=dag
        ) \
        >> end("end_unic_publish_project")
