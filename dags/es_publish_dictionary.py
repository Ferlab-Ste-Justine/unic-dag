"""
Génération des DAGs pour le publication du dictionnaire d'un projet dans le Portail de l'UNIC.
Un DAG par environnement postgres est généré.
"""
# pylint: disable=missing-function-docstring, invalid-name, expression-not-assigned

from datetime import datetime
from typing import List

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.exceptions import AirflowFailException
from airflow.models import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule

from lib.hooks.postgresca import PostgresCaHook
from lib.tasks.excel import parquet_to_excel
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

def get_resource_code() -> str:
    return "{{ params.resource_code or '' }}"

def get_dict_version() -> str:
    return "{{ params.dict_version or '' }}"

def get_version_to_publish() -> str:
    return "{{ params.version_to_publish or '' }}"

def get_release_id() -> str:
    return '{{ params.release_id or "" }}'

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
    conn_id = unic_postgres_vlan2_conn_id(env)

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
                "branch": Param("master", type="string"),
                "resource_code": Param("", type="string", description="Resource to publish."),
                "dict_version": Param("", type="string", description="Version of dictionary. Use semantic versioning. Ex: v1.0.0"),
                "version_to_publish": Param("", type="string", description="Date to publish. Ex: 2001-01-01"),
                "release_id": Param("", type=["null", "string"], description="Release id of OpenSearch index. If not specified, will increment. Ex: re_0000")
            },
            default_args=dag_args,
            doc_md=doc,
            start_date=datetime(2024, 12, 16),
            is_paused_upon_creation=False,
            schedule_interval=None,
            tags=["postgresql", "published", "opensearch"],
            on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
    ) as dag:
        cur_dict_version = get_dict_version()
        resource_code = get_resource_code()
        version_to_publish = get_version_to_publish()
        # pg_hook = PostgresCaHook(postgres_conn_id=conn_id, ca_path=postgres_vlan2_ca_path,
        #                          ca_filename=postgres_ca_filename, ca_cert=postgres_vlan2_ca_cert)

        # @task(task_id='get_dict_current_version')
        # def get_dict_current_version() -> str:
        #     if resource_code == "":
        #         raise AirflowFailException("DAG param 'resource_code' is required.")
        #     else:
        #         try:
        #             res = pg_hook.get_first(sql=f"""SELECT dict_current_version FROM catalog.resource WHERE code={resource_code}""")
        #             return res[0]
        #         except Exception as e:
        #             raise AirflowFailException(f"Failed to get current dict version: {e}")
        #
        # @task(task_id='validate_version')
        # def validate_version(prev_dict_version : str) -> bool:
        #     if cur_dict_version == "":
        #         raise AirflowFailException("DAG param 'dict_version' is required.")
        #     elif Version(prev_dict_version) > Version(cur_dict_version):
        #         raise AirflowFailException(
        #         f"DAG param 'dict_version' must be larger or equal to previous dict version: {prev_dict_version}")
        #     elif Version(prev_dict_version) == Version(cur_dict_version):
        #         # Publish without updating Dictionary
        #         return False
        #     else:
        #         # Publish data, Dictionary, and Re-Index OpenSearch
        #         return True
        #
        # @task(task_id='update_dict_current_version')
        # def update_dict_current_version() -> None:
        #     query = f"""
        #         UPDATE catalog.resource
        #         SET dict_current_version = {cur_dict_version}
        #         WHERE course_id = {resource_code};
        #     """
        #     try:
        #         pg_hook.run(query)
        #     except Exception as error:
        #         print("failed: ")
        #         print (error)
        #
        # def publish_dictionary_task(publish : bool) -> None:
        #     if publish:
        #         publish_dictionary.override(task_id=f"publish_dictionary_{resource_code}")(
        #             pg_hook,
        #             get_resource_code(),
        #             f"published/{resource_code}/{resource_code}_dictionary_{cur_dict_version}.xlsx",
        #             GREEN_BUCKET
        #         )

        @task_group(group_id="publish_tables")
        def publish_tables() -> None:
            s3 = S3Hook(aws_conn_id=green_minio_conn_id)

            keys = s3.list_keys(bucket_name=GREEN_BUCKET, prefix=f"released/{resource_code}/{version_to_publish}/")

            publish_tasks = [
                parquet_to_excel.override(task_id=f"publish_{resource_code}_{key}")(
                    GREEN_BUCKET,
                    f"released/{resource_code}/{version_to_publish}/{key}",
                    GREEN_BUCKET,
                    f"published/{resource_code}/{version_to_publish}/{key}.xlsx"
            ) for key in keys]

            publish_tasks



        # get_dict_current_version_task = get_dict_current_version
        # validate_version_task = validate_version(prev_dict_version=get_dict_current_version_task)
        #
        # start("start_es_publish_dictionary") >> get_dict_current_version_task \
        # >> validate_version_task(prev_dict_version=get_dict_current_version_task) \
        # >> publish_dictionary_task(publish=validate_version_task) \
        # >> end("end_es_publish_dictionary")

        start("start_es_publish_dictionary") \
        >> publish_tables() \
        >> end("end_es_publish_dictionary")






