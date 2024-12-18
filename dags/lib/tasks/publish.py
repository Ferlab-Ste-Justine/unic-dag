import os
from typing import re

import pandas as pd
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

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