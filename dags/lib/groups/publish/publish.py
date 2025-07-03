from airflow.decorators import task_group

from lib.opensearch import OpensearchAlias, OpensearchEnv
from lib.tasks.opensearch import prepare_index, load_index, publish_index, get_next_release_id
from lib.tasks.notify import start, end
from lib.config import PUBLISHED_BUCKET, YELLOW_MINIO_CONN_ID

from lib.tasks.publish import get_resource_code, get_version_to_publish, get_include_dictionary, update_dict_current_version, publish_dictionary, get_publish_kwargs
from lib.tasks.excel import parquet_to_excel

@task_group(group_id="publish_research_project")
def publish_research_project(pg_conn_id: str, resource_code: str, version_to_publish: str, include_dictionary: bool) -> None:

    update_dict_current_version(dict_version=version_to_publish, resource_code=resource_code, include_dictionary=include_dictionary, pg_conn_id=pg_conn_id) \
    >> publish_dictionary(resource_code=resource_code, version_to_publish=version_to_publish, include_dictionary=include_dictionary, pg_conn_id=pg_conn_id) \
    >> parquet_to_excel.override(task_id="publish_project_data").expand_kwargs(get_publish_kwargs(
        resource_code=resource_code,
        version_to_publish=version_to_publish,
        minio_conn_id=YELLOW_MINIO_CONN_ID,
        bucket=PUBLISHED_BUCKET
    ))