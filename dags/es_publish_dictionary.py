"""
Génération des DAGs pour le publication du dictionnaire d'un projet dans le Portail de l'UNIC.
Un DAG par environnement postgres est généré.
"""

# pylint: disable=missing-function-docstring, invalid-name, expression-not-assigned, raise-missing-from, cell-var-from-loop

from datetime import datetime
from typing import List

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models import Param, DagRun
from airflow.utils.trigger_rule import TriggerRule

from lib.tasks.publish import released_to_published
from lib.hooks.postgresca import PostgresCaHook
from lib.tasks.notify import start, end
from lib.tasks.opensearch import publish_dictionary
from lib.config import default_args, os_url, os_port, green_minio_conn_id
from lib.postgres import postgres_vlan2_ca_path, postgres_vlan2_ca_cert, PostgresEnv, unic_postgres_vlan2_conn_id, \
    postgres_ca_filename
from lib.slack import Slack

from packaging.version import Version

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

for env in PostgresEnv:
    env_name = env.value
    pg_conn_id = unic_postgres_vlan2_conn_id(env)

    doc = f"""
    # Publish table to Unic Portal
    
    DAG pour la publication d'un projet dans le portail de l'UNIC dans l'environnement **{env_name}**.
    
    ### Description
    Ce DAG publie le dictionaire d'un projet dans la zone published de minio et inclue le projet dans les indexs 
    opensearch du portail Unic. Il a un dag par environnment postgres.
    
    ### Configuration
    * Paramètre `branch` : Branche du jar à utiliser.
    * Paramètre `resource_code` : Code de la ressource à charger.
    * Paramètre `dict_version` : version du dictionnaire à publier dans published.
    * Paramètre `version_to_publish` : date version dans released à publier dans published.
    * Paramètre `release_id` (optional) : release id of opensearch index

    """

    with DAG(
            dag_id=f"es_publish_{env_name}_dictionary",
            params={
                "resource_code": Param("", type="string", description="Resource to publish."),
                "dict_version": Param("", type="string", description="Version of dictionary. Use semantic versioning. Ex: v1.0.0"),
                "version_to_publish": Param("", type="string", description="Date to publish. Ex: 2001-01-01"),
                "release_id": Param("", type=["null", "string"], description="Release id of OpenSearch index. If not specified, will increment. Ex: re_0000")
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
            return dag_run.conf['resource_code']

        @task
        def get_dict_version(ti=None) -> str:
            dag_run: DagRun = ti.dag_run
            return dag_run.conf['dict_version']

        @task
        def get_version_to_publish(ti=None) -> str:
            dag_run: DagRun = ti.dag_run
            return dag_run.conf['version_to_publish']

        @task
        def get_release_id(ti=None) -> str:
            dag_run: DagRun = ti.dag_run
            return dag_run.conf['release_id']


        pg_hook = PostgresCaHook(postgres_conn_id=pg_conn_id, ca_path=postgres_vlan2_ca_path,
                                 ca_filename=postgres_ca_filename, ca_cert=postgres_vlan2_ca_cert)

        @task(task_id='get_dict_prev_version')
        def get_dict_prev_version(resource_code=str) -> str:
            if resource_code == "":
                raise AirflowFailException("DAG param 'resource_code' is required.")

            try:
                res = pg_hook.get_first(sql=f"""SELECT dict_current_version FROM catalog.resource WHERE code={resource_code}""")
                return res[0]
            except Exception as e:
                raise AirflowFailException(f"Failed to get prev dict version: {e}")

        @task(task_id='validate_version')
        def validate_version(prev_dict_version : str, cur_dict_version : str) -> bool:
            if cur_dict_version == "":
                raise AirflowFailException("DAG param 'dict_version' is required.")

            if Version(prev_dict_version) > Version(cur_dict_version):
                raise AirflowFailException(
                f"DAG param 'dict_version' must be larger or equal to previous dict version: {prev_dict_version}")

            if Version(prev_dict_version) == Version(cur_dict_version):
                # Publish without updating Dictionary
                return False

            # Publish data, Dictionary, and Re-Index OpenSearch
            return True

        @task(task_id='update_dict_current_version')
        def update_dict_current_version(cur_dict_version : str, resource_code : str) -> None:
            query = f"""
                UPDATE catalog.resource
                SET dict_current_version = {cur_dict_version}
                WHERE course_id = {resource_code};
            """

            pg_hook.run(query)


        @task.branch(task_id="dict_to_be_updated")
        def dict_to_be_updated(publish : bool):
            if publish:
                return "publish_dictionary"

            return "released_to_published"

        @task(task_id="publish_dictionary")
        def publish_dictionary_task(cur_dict_version : str, resource_code : str):
            return publish_dictionary(
                pg_hook,
                get_resource_code(),
                f"published/{resource_code}/{resource_code}_dictionary_{cur_dict_version}.xlsx",
                GREEN_BUCKET,
                minio_conn_id=green_minio_conn_id
            )

        @task(task_id="released_to_published")
        def released_to_published_task(cur_dict_version : str, resource_code : str, version_to_be_published : str):
            return released_to_published(
                resource_code,
                version_to_be_published,
                cur_dict_version,
                GREEN_BUCKET
            )

        get_resource_code_task = get_resource_code()
        get_dict_version_task = get_dict_version()
        get_version_to_publish_task = get_version_to_publish()
        # get_release_id_task = get_release_id()

        get_dict_prev_version_task = get_dict_prev_version(resource_code=get_resource_code_task)
        validate_version_task = validate_version(prev_dict_version=get_dict_prev_version_task, cur_dict_version=get_dict_version_task)
        publish_dictionary_task = publish_dictionary_task(cur_dict_version=get_dict_version_task, resource_code=get_resource_code_task)
        released_to_published_task = released_to_published_task(
            cur_dict_version=get_dict_version_task, resource_code=get_resource_code_task, version_to_be_published=get_version_to_publish_task
        )

        start("start_es_publish_dictionary") >> [get_resource_code_task, get_dict_version_task, get_version_to_publish_task] \
        >> get_dict_prev_version_task \
        >> validate_version_task \
        >> dict_to_be_updated(publish=validate_version_task) \
        >> [publish_dictionary_task, released_to_published_task]

        publish_dictionary_task >> released_to_published_task

        released_to_published_task >> end("end_es_publish_dictionary")
