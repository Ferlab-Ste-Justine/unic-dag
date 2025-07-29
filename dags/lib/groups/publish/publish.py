from airflow.decorators import task_group

from lib.opensearch import OpensearchAlias, OpensearchEnv
from lib.tasks.opensearch import prepare_index, load_index, publish_index, get_next_release_id
from lib.tasks.notify import start, end
from lib.config import PUBLISHED_BUCKET, YELLOW_MINIO_CONN_ID

from lib.tasks.publish import get_resource_code, get_version_to_publish, get_include_dictionary, update_dict_current_version, publish_dictionary, get_publish_kwargs, extract_config_info
from lib.tasks.excel import parquet_to_excel


@task_group(group_id="publish_research_project")
def publish_research_project(pg_conn_id: str, resource_code: str, version_to_publish: str, include_dictionary: bool) -> None:

    dictionary_update_task = update_dict_current_version(dict_version=version_to_publish, resource_code=resource_code, include_dictionary=include_dictionary, pg_conn_id=pg_conn_id)

    #Extracting config info relevant for this resource code
    config_info = extract_config_info(resource_code=resource_code,
                                      version_to_publish=version_to_publish,
                                      minio_conn_id=YELLOW_MINIO_CONN_ID,
                                      bucket=PUBLISHED_BUCKET)

    dict_task = publish_dictionary(
        resource_code=resource_code,
        version_to_publish=version_to_publish,
        include_dictionary=include_dictionary,
        pg_conn_id=pg_conn_id,
        config=config_info,
        s3_destination_bucket=PUBLISHED_BUCKET)

    publish_task = parquet_to_excel.override(task_id="publish_project_data").expand_kwargs(get_publish_kwargs(
        resource_code=resource_code,
        version_to_publish=version_to_publish,
        minio_conn_id=YELLOW_MINIO_CONN_ID,
        config=config_info
    ))

    #Set up task order
    start() >> dictionary_update_task >> config_info >> dict_task >> publish_task >> end()