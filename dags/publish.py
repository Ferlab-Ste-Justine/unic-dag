"""
Génération des DAGs pour le publication du dictionnaire d'un projet dans le Portail de l'UNIC.
Un DAG par environnement postgres est généré.
"""

# pylint: disable=missing-function-docstring, invalid-name, expression-not-assigned, raise-missing-from, cell-var-from-loop
import re

from datetime import datetime
from typing import List

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models import Param, DagRun
from airflow.utils.trigger_rule import TriggerRule

from lib.tasks.publish import publish_dictionary, update_dict_current_version, released_to_published
from lib.tasks.notify import start, end
from lib.config import default_args
from lib.postgres import PostgresEnv, unic_postgres_vlan2_conn_id
from lib.slack import Slack

ZONE = "green"
SUBZONE = "published"
MAIN_CLASS = "bio.ferlab.ui.etl.green.published.Main"
GREEN_BUCKET = "green-prd"
env_name = None
conn_id = None

# Update default args
dag_args = default_args.copy()
dag_args.update({
    'trigger_rule': TriggerRule.NONE_FAILED,
    'on_failure_callback': Slack.notify_task_failure})

def prepare_index_arguments(task_id: str) -> List[str]:
    return [
        task_id,
        "--config", "config/prod.conf",
        "--steps", "default",
        "--app-name", f"prepare_{task_id}",
        "--env", env_name
    ]

for env in PostgresEnv:
    env_name = env.value
    pg_conn_id = unic_postgres_vlan2_conn_id(env)

    doc = f"""
    # Publish Reasearch Project
    
    DAG pour la publication d'un projet de recherche dans le portail de l'UNIC dans l'environnement **{env_name}**.
    
    ### Description
    Ce DAG publie le dictionaire d'un projet dans la zone published de minio et inclue le projet dans les indexs 
    opensearch du portail UNIC. Il a un dag par environnment postgres.
    
    ### Configuration
    * Paramètre `resource_code` : Code de la ressource à charger.
    * Paramètre `version_to_publish` : date version dans released à publier dans published.
    * Paramètre `release_id` (optional) : release id of opensearch index

    """

    with DAG(
            dag_id=f"os_publish_{env_name}_dictionary",
            params={
                "resource_code": Param("", type="string", description="Resource to publish. Ex: cpip"),
                "version_to_publish": Param("", type="string", description="Date to publish. If not specified, will take most recent release. Ex: 2001-01-01"),
                "release_id": Param("", type=["null", "string"], description="(Optional) Release id of OpenSearch index. If not specified, will increment. Ex: re_0000")
            },
            default_args=dag_args,
            doc_md=doc,
            start_date=datetime(2024, 12, 16),
            is_paused_upon_creation=False,
            render_template_as_native_obj=True,
            schedule_interval=None,
            tags=["postgresql", "published", "opensearch"],
            on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
    ) as dag:
        @task
        def get_resource_code(ti=None) -> str:
            dag_run: DagRun = ti.dag_run
            resource_code = dag_run.conf['resource_code']

            if not resource_code:
                raise AirflowFailException("DAG param 'resource_code' is required.")
            else:
                return resource_code

        @task
        def get_version_to_publish(ti=None) -> str:
            dag_run: DagRun = ti.dag_run
            version_to_publish = dag_run.conf['version_to_publish']
            date_format = "%Y-%m-%d"
            if bool(datetime.strptime(version_to_publish, date_format)):
                return dag_run.conf['version_to_publish']
            elif not version_to_publish:
                raise AirflowFailException(f"DAG param 'version_to_publish' is required. Expected format: YYYY-MM-DD")
            else:
                raise AirflowFailException(f"DAG param 'version_to_publish' is not in the correct format. Expected format: YYYY-MM-DD")

        @task
        def get_release_id(ti=None) -> str:
            dag_run: DagRun = ti.dag_run
            release_id = dag_run.conf['release_id']
            regex = "^re_\d{4}$"
            if not release_id:
                return release_id
            elif re.fullmatch(regex, release_id):
                return dag_run.conf['release_id']
            else:
                raise AirflowFailException(f"Param version_to_publish is not in the correct format. Expected format: YYYY-MM-DD")

        get_resource_code_task = get_resource_code()
        get_version_to_publish_task = get_version_to_publish()
        get_release_id_task = get_release_id()

        start("start_os_publish_dictionary") >> [get_resource_code_task, get_version_to_publish_task, get_release_id_task] \
        >> update_dict_current_version(cur_dict_version=get_version_to_publish_task, resource_code=get_resource_code_task, pg_conn_id=pg_conn_id) \
        >> publish_dictionary(resource_code=get_resource_code(), pg_conn_id=pg_conn_id)
        >> released_to_published(resource_code=get_resource_code()) \
        >> end("end_os_publish_dictionary")

        # TODO: put all of the load index tasks in a task group to reuse in this dag and others
