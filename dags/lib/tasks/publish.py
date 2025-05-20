import logging
import os

import pandas as pd
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from lib.hooks.postgresca import PostgresCaHook

from lib.postgres import get_pg_ca_hook
from lib.publish import resource_query, dict_table_query, variable_query, value_set_query, value_set_code_query, mapping_query
from lib.config import GREEN_BUCKET, green_minio_conn_id

@task(task_id="publish_dictionary",)
def publish_dictionary(
        resource_code: str,
        pg_conn_id: str,
        s3_destination_bucket: str = GREEN_BUCKET,
        minio_conn_id: str = green_minio_conn_id) -> None:
    """
    Publish research project dictionary.

    :param resource_code: resource code of project to publish.
    :param pg_conn_id: Postgres connection id.
    :param s3_destination_bucket: published bucket name.
    :param minio_conn_id: Minio connection id.
    :return: None
    """

    # define excel vars
    local_excel_directory = '/tmp/excel/'

    # define connection vars
    s3 = S3Hook(aws_conn_id=minio_conn_id)
    pg = get_pg_ca_hook(pg_conn_id=pg_conn_id)

    result_map = {
        "Resource" : pg.get_pandas_df(resource_query(resource_code)),
        "Dict Tables" : pg.get_pandas_df(dict_table_query(resource_code)),
        "Variable" : pg.get_pandas_df(variable_query(resource_code)),
        "Value Sets" : pg.get_pandas_df(value_set_query(resource_code)),
        "Value Set Codes" : pg.get_pandas_df(value_set_code_query(resource_code)),
        "Mappings" : pg.get_pandas_df(mapping_query(resource_code))
    }

    local_excel_file = os.path.join(local_excel_directory, os.path.basename(resource_code))

    # convert to excel
    try:
         with pd.ExcelWriter(local_excel_file) as excel_writer:
            for sheet, data in result_map.items():
                data.to_excel(excel_writer, sheet_name=sheet, index=False)
    except Exception as e:
        raise AirflowFailException(f"Failed to convert {local_excel_file} to excel: {e}")

    # Upload to minio
    try:
        s3.load_file(local_excel_file, key=resource_code, bucket_name=s3_destination_bucket, replace=True)
    except Exception as e:
        raise AirflowFailException(f"Failed to upload Excel file {local_excel_file} to minio: {e}")


@task(task_id='update_dict_current_version')
def update_dict_current_version(dict_version: str, resource_code: str, pg_conn_id: str) -> None:
    """
    Update dict version for a given resource.

    :param dict_version: New dict version to set.
    :param resource_code: Resource code associated to the dict.
    :param pg_conn_id: Postgres connection id.
    :return: None
    """
    pg_hook = get_pg_ca_hook(pg_conn_id)

    query = f"""
                UPDATE catalog.resource
                SET dict_current_version = '{dict_version}'
                WHERE recourse_id = '{resource_code}';
            """

    pg_hook.run(query)