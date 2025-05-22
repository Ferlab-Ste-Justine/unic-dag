from airflow.decorators import task_group

from lib.opensearch import OpensearchAlias, OpensearchEnv
from lib.tasks.opensearch import prepare_index, load_index, publish_index, get_next_release_id
from lib.tasks.notify import start, end
from lib.config import PUBLISHED_BUCKET

from lib.tasks.publish import get_resource_code, get_version_to_publish, get_release_id, update_dict_current_version, publish_dictionary, get_publish_kwargs
from lib.tasks.excel import parquet_to_excel

@task_group(group_id="publish_research_project")
def publish_research_project(pg_conn_id: str):
    get_resource_code_task = get_resource_code()
    get_version_to_publish_task = get_version_to_publish()

    [get_resource_code_task, get_version_to_publish_task] \
    >> update_dict_current_version(dict_version=get_version_to_publish_task, resource_code=get_resource_code_task, pg_conn_id=pg_conn_id) \
    >> publish_dictionary(resource_code=get_resource_code_task, version_to_publish=get_version_to_publish_task, pg_conn_id=pg_conn_id) \
    >> parquet_to_excel.override(task_id="publish_project_data").expand_kwargs(get_publish_kwargs(
        resource_code=get_resource_code_task,
        version_to_publish=get_version_to_publish_task,
        minio_conn_id='minio',
        bucket=PUBLISHED_BUCKET
    ))