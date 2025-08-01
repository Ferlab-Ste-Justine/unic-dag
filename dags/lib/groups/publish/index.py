from typing import List

from airflow import DAG
from airflow.decorators import task_group

from lib.config import SPARK_FAILURE_MSG, MASTER_JAR
from lib.opensearch import OpensearchAlias
from lib.tasks.opensearch import prepare_index, load_index, publish_index, get_next_release_id


@task_group(group_id="index_opensearch")
def index_opensearch(pg_env_name: str, os_env_name: str, dag: DAG, skip: bool = False):
    def get_release_id() -> str:
        return '{{ params.release_id or "" }}'

    def prepare_index_arguments(task_id: str) -> List[str]:
        return [
            task_id,
            "--config", "config/prod.conf",
            "--steps", "initial",
            "--app-name", f"prepare_{task_id}",
            "--env", pg_env_name
        ]

    @task_group(group_id="prepare_indexes")
    def prepare_index_group():
        for alias in OpensearchAlias:
            task_id = f"os_index_{alias.value}"
            prepare_index(
                task_id=task_id,
                args=prepare_index_arguments(task_id),
                jar=MASTER_JAR,
                spark_failure_msg=SPARK_FAILURE_MSG,
                cluster_size="small-etl",
                dag=dag,
                skip=skip
            )

    @task_group(group_id="load_indexes")
    def load_index_group(release_id: str):
        for alias in OpensearchAlias:
            load_index.override(task_id=f"load_index_{alias.value}")(
                env_name=os_env_name,
                release_id=release_id,
                alias=alias.value,
                src_path=f"catalog/{pg_env_name}/os_index/",
                skip=skip
            )

    @task_group(group_id="publish_indexes")
    def publish_index_group(release_id: str):
        for alias in OpensearchAlias:
            publish_index.override(task_id=f"publish_index_{alias.value}")(
                env_name=os_env_name,
                release_id=release_id,
                alias=alias.value,
                skip=skip
            )

    get_next_release_id_task = get_next_release_id(env_name=os_env_name, release_id=get_release_id(), skip=skip)

    prepare_index_group() >> get_next_release_id_task >> load_index_group(release_id=get_next_release_id_task) \
    >> publish_index_group(release_id=get_next_release_id_task)
