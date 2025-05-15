import os

import pandas as pd
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from lib.hooks.postgresca import PostgresCaHook

from lib.postgres import get_pg_ca_hook
from lib.config import GREEN_BUCKET, green_minio_conn_id


@task(task_id="publish_dictionary",)
def publish_dictionary(
        resource_code: str,
        pg_conn_id: str,
        s3_destination_bucket: str = GREEN_BUCKET,
        minio_conn_id: str = green_minio_conn_id) -> None:

    # define excel vars
    local_excel_directory = '/tmp/excel/'

    # define minio vars
    s3 = S3Hook(aws_conn_id=minio_conn_id)

    # define sql queries
    resourse_query = f"""
        SELECT * FROM catalog.resource 
        WHERE code='{resource_code}'
        AND r.to_be_published = true
    """

    dict_table_query = f"""
        SELECT t.*
        FROM catalog.dict_table t
        INNER JOIN catalog.resource r
        ON t.resource_id = r.id
        WHERE r.code='{resource_code}'
        AND r.to_be_published = true
        AND t.to_be_published = true
    """

    variable_query = f"""
        WITH filt_dict_table AS (
            SELECT t.id AS filt_table_id 
            FROM catalog.dict_table t
            INNER JOIN catalog.resource r
            ON t.resource_id = r.id
            WHERE r.code='{resource_code}'
            AND r.to_be_published = true
            AND t.to_be_published = true
        )
        SELECT v.* 
        FROM catalog.variable v
        INNER JOIN filt_dict_table fdt
        ON fdt.filt_table_id = v.table_id
        WHERE v.to_be_published = true
    """

    value_set_query = f"""
        WITH filt_dict_table AS (
            SELECT t.id AS filt_table_id 
            FROM catalog.dict_table t
            INNER JOIN catalog.resource r
            ON t.resource_id = r.id
            where r.code='{resource_code}'
            AND r.to_be_published = true
            AND t.to_be_published = true
        ),
        filt_variable AS (
            SELECT v.value_set_id AS filt_value_set_id
            FROM catalog.variable v
            INNER JOIN filt_dict_table fdt
            ON fdt.filt_table_id = v.table_id
            WHERE v.to_be_published = true
        )
        SELECT vs.* 
        FROM catalog.value_set vs
        INNER JOINfilt_variable fv
        ON fv.filt_value_set_id = vs.id
    """

    value_set_code_query = f"""
        WITH filt_dict_table AS (
            SELECT t.id AS filt_table_id 
            FROM catalog.dict_table t
            INNER JOIN catalog.resource r
            ON t.resource_id = r.id
            WHERE r.code='{resource_code}'
            AND r.to_be_published = true
            AND t.to_be_published = true
        ),
        filt_variable as (
            SELECT v.value_set_id AS filt_value_set_id
            FROM catalog.variable v
            INNER JOIN filt_dict_table fdt
            ON fdt.filt_table_id = v.table_id
            WHERE v.to_be_published = true
        )
        SELECT vsc.* 
        FROM catalog.value_set_code vsc
        INNER JOIN filt_variable fv
        ON fv.filt_value_set_id = vsc.value_set_id
    """

    mapping_query = f"""
        WITH filt_dict_table AS (
            SELECT t.id AS filt_table_id 
            FROM catalog.dict_table t
            INNER JOIN catalog.resource r
            ON t.resource_id = r.id
            WHERE r.code='{resource_code}'
            AND r.to_be_published = true
            AND t.to_be_published = true
        ),
        filt_variable AS (
            SELECT v.value_set_id AS filt_value_set_id
            FROM catalog.variable v
            INNER JOIN filt_dict_table fdt
            ON fdt.filt_table_id = v.table_id
            WHERE v.to_be_published = true
        ),
        filt_value_set_code AS (
            SELECT vsc.id AS filt_value_set_code_id
            FROM catalog.value_set_code vsc
            INNER JOIN filt_variable fv
            ON fv.filt_value_set_id = vsc.value_set_id
        )
        SELECT m.* 
        FROM catalog.mapping m
        INNER JOIN filt_value_set_code fvsc
        ON fvsc.filt_value_set_code_id = m.value_set_code_id
    """

    result_map = {
        "Resource" : pg_hook.get_pandas_df(resourse_query),
        "Dict Tables" : pg_hook.get_pandas_df(dict_table_query),
        "Variable" : pg_hook.get_pandas_df(variable_query),
        "Value Sets" : pg_hook.get_pandas_df(value_set_query),
        "Value Set Codes" : pg_hook.get_pandas_df(value_set_code_query),
        "Mappings" : pg_hook.get_pandas_df(mapping_query)
    }


    local_excel_file = os.path.join(local_excel_directory, os.path.basename(s3_destination_key))

    # convert to excel
    try:
        excel_writer = pd.ExcelWriter(local_excel_file)
        for sheet, data in result_map.items():
            data.to_excel(excel_writer, sheet_name=sheet, index=False)
        excel_writer.close()
    except Exception as e:
        raise AirflowFailException(f"Failed to convert {local_excel_file} to excel: {e}")

    # Upload to minio
    try:
        s3.load_file(local_excel_file, key=s3_destination_key, bucket_name=s3_destination_bucket, replace=True)
    except Exception as e:
        raise AirflowFailException(f"Failed to upload Excel file {local_excel_file} to minio: {e}")

@task(task_id='update_dict_current_version')
def released_to_published(resource_code: str) -> None:
    pass

@task(task_id='update_dict_current_version')
def update_dict_current_version(cur_dict_version: str, resource_code: str, pg_conn_id: str) -> None:
    pg_hook = get_pg_ca_hook(pg_conn_id)

    query = f"""
                UPDATE catalog.resource
                SET dict_current_version = {cur_dict_version}
                WHERE recourse_id = {resource_code};
            """

    pg_hook.run(query)