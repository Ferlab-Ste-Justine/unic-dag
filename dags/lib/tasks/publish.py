import os
from typing import re

import pandas as pd
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def publish_dictionary(
        pg_hook: PostgresCaHook,
        resource_code : str,
        s3_destination_key : str,
        s3_destination_bucket : str,
        minio_conn_id: str = 'minio_conn_id') -> None:

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

@task(task_id="released_to_published")
def released_to_published(
        resource_code : str,
        version_to_publish: str,
        dict_version : str,
        bucket : str = 'green-prd',
        header : str = None,
        sheet_name: str = "sheet1",
        minio_conn_id: str = 'minio_conn_id') -> None:
    """
    Create an Airflow task to take all tables of specified resource and version in released (parquet) and publish them (xlsx).

    Params:
    - resource_code: name of resource to publish.
    - version_to_publish:  version in released to publish (ex: 2020-01-01).
    - bucket:   realed to publish bucket.
    - header:               list of column names to use as header in the Excel file, default -> None.
    - sheet_name:           name of the sheet in the Excel file, default -> 'Sheet1'.
    - minio_conn_id:        connection id service, default -> 'minio_conn_id'.

    Returns:
    - function: An Airflow task to be used in DAGs for publishing a version of a resource.
    """
    
    s3 = S3Hook(aws_conn_id=minio_conn_id)
    s3_client = s3.get_conn()

    # Define local dirs
    local_parquet_directory = '/tmp/parquet/'
    local_excel_directory = '/tmp/excel'

    # Create dirs if they do not exist
    os.makedirs(local_parquet_directory, exist_ok=True)
    os.makedirs(local_excel_directory, exist_ok=True)

    # Get all tables to publish
    keys = s3.list_keys(bucket_name=bucket, prefix=f"released/{resource_code}/{version_to_publish}/")
    tables = [re.search(f'{version_to_publish}/(.+?)/', key).group(1) for key in keys]

    # Publish each table
    for table in tables:
        source = f"released/{resource_code}/{version_to_publish}/{table}",
        dest = f"published/{resource_code}/{version_to_publish}/{table}.xlsx"

        try:
            keys = s3.list_keys(bucket_name=bucket, prefix=source)
            if not keys:
                raise AirflowFailException(f"No files found in: {bucket}/{source}")
        except Exception as e:
            raise AirflowFailException(f"Failed to list the files from: {bucket}/{source}: {e}")

        # Download parquet files
        parquet_files = []
        for key in keys:
            if key.endswith('.parquet'):
                local_file_path = os.path.join(local_parquet_directory, os.path.basename(key))
                try:
                    s3_client.download_file(bucket, key, local_file_path)
                    parquet_files.append(local_file_path)
                except Exception as e:
                    raise AirflowFailException(f"Failed to download the file: {bucket}/{key}: {e}")

        if not parquet_files:
            raise AirflowFailException(f'No parquet files found in: {bucket}/{source}')

        # Combine Parquet files into a single dataframe
        try:
            df = pd.concat([pd.read_parquet(file) for file in parquet_files], ignore_index=True)
        except Exception as e:
            raise AirflowFailException(f"Failed to combine parquet files into single df: {e}")

        # Set the header if not provided
        if header is None:
            header = df.columns

        # Save the output to xlsx format
        local_excel_file = os.path.join(local_excel_directory, os.path.basename(dest))
        try:
            df.to_excel(local_excel_file, index=False, header=header, sheet_name=sheet_name)
        except Exception as e:
            raise AirflowFailException(f"Failed to convert {local_excel_file} to excel: {e}")

        # Upload to minio
        try:
            s3.load_file(local_excel_file, key=dest, bucket_name=bucket, replace=True)
        except Exception as e:
            raise AirflowFailException(f"Failed to upload Excel file {local_excel_file} to bucket {bucket}: {e}")