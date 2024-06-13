import os
import glob
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


@task(task_id="parquet_to_excel")
def parquet_to_excel(
    parquet_bucket_name: str,
    parquet_dir_key: str,
    excel_bucket_name: str,
    excel_output_key: str,
    header: list = None,
    sheet_name: str = "sheet1",
    minio_conn_id: str = 'minio_conn_id') -> None:

    """
    Create an Airflow task to convert multiple or single parquet from a specified directory from Minio into a single excel file.
    Output stored in another specified directory.

    Params:
    - parquet_bucket_name:  bucket where the parquet files are stored.
    - parquet_dir_key:      directory prefix inside the bucket where parquet files are located.
    - excel_bucket_name:    bucket where the output excel files will be stored.
    - excel_output_key:     directory prefix where the output excel file(s) will be stored.
    - header:               list of column names to use as header in the Excel file.
    - sheet_name:           name of the sheet in the Excel file, default -> 'Sheet1'.
    - minio_conn_id:        connection id service, default -> 'minio_conn_id'.

    Returns:
    - function: An Airflow task to be used in DAGs for converting parquet to excel.
    """

    s3 = S3Hook(aws_conn_id=minio_conn_id)
    s3_client = s3.get_conn()

    # Define local dirs
    local_parquet_directory = '/tmp/parquet/'
    local_excel_directory = '/tmp/excel'
    
    # Create dirs if they do not exist
    os.makedirs(local_parquet_directory, exist_ok=True)
    os.makedirs(local_excel_directory, exist_ok=True)

    # List all the files for a given dir in Minio
    try:
        keys = s3.list_keys(bucket_name=parquet_bucket_name, prefix=parquet_dir_key)
        if not keys:
            raise AirflowFailException(f"No files found in: {parquet_bucket_name}/{parquet_dir_key}")
    except Exception as e:
        raise AirflowFailException(f"Failed to list the files from: {parquet_bucket_name}/{parquet_dir_key}: {e}")

    # Download parquet files
    parquet_files = []
    for key in keys:
        if key.endswith('.parquet'):
            local_file_path = os.path.join(local_parquet_directory, os.path.basename(key))
            try:
                s3_client.download_file(parquet_bucket_name, key, local_file_path)
                parquet_files.append(local_file_path)
            except Exception as e:
                raise AirflowFailException(f"Failed to download the file: {parquet_bucket_name}/{key}: {e}")

    if not parquet_files:
        raise AirflowFailException(f'No parquet files found in: {parquet_bucket_name}/{parquet_dir_key}')
    
    # Combine Parquet files into a single dataframe
    try:
        df = pd.concat([pd.read_parquet(file) for file in parquet_files], ignore_index=True)
    except Exception as e:
        raise AirflowFailException(f"Failed to combine parquet files into single df: {e}")

    # Save the output to xlsx format
    local_excel_file = os.path.join(local_excel_directory, os.path.basename(excel_output_key))
    try:
        df.to_excel(local_excel_file, index=False, header=header, sheet_name=sheet_name)
        if header is None:
            header = df.columns
    except Exception as e:
        raise AirflowFailException(f"Failed to convert {local_excel_file} to excel: {e}")

    # Upload to minio
    try:
        s3.load_file(local_excel_file, key=excel_output_key, bucket_name=excel_bucket_name, replace=True)
    except Exception as e:
        raise AirflowFailException(f"Failed to upload Excel file {local_excel_file} to bucket {excel_bucket_name}: {e}")


@task(task_id="csv_to_excel")
def csv_to_excel(
    csv_bucket_name: str,
    csv_dir_key: str,
    excel_bucket_name: str,
    excel_output_key: str,
    header: list = None,
    sheet_name: str = "sheet1",
    minio_conn_id: str = 'minio_conn_id') -> None:

    """
    Create an Airflow task to convert multiple or single csv from a specified directory from Minio.
    Each output file is stored in another specified directory.

    Params:
    - csv_bucket_name:      bucket where the CSV files are stored.
    - csv_dir_key:          directory prefix inside the bucket where csv files are located.
    - excel_bucket_name:    bucket where the output excel files will be stored.
    - excel_dir_key:        directory prefix where the output excel file(s) will be stored.
    - header:               list of column names to use as header in the Excel file.
    - sheet_name:           name of the sheet in the Excel file, default -> 'Sheet1'.
    - minio_conn_id:        connection id service, default -> 'minio_conn_id'.

    Returns:
    - function: An Airflow task to be used in DAGs for converting csv to excel.
    """

    s3 = S3Hook(aws_conn_id=minio_conn_id)
    s3_client = s3.get_conn()

    # Define local dirs
    local_csv_directory = '/tmp/csv/'
    local_excel_directory = '/tmp/excel/'
    
    # Create dirs if they do not exist
    os.makedirs(local_csv_directory, exist_ok=True)
    os.makedirs(local_excel_directory, exist_ok=True)

    # List all the files for a given dir in Minio
    keys = s3.list_keys(bucket_name=csv_bucket_name, prefix=csv_dir_key)

    if not keys:
        raise AirflowFailException(f"No files found in {csv_bucket_name}/{csv_dir_key}")

    csv_files = []
    for csv in keys:
        if csv.endswith('.csv'):
            local_csv_file = os.path.join(local_csv_directory, os.path.basename(csv))
            try:
                s3_client.download_file(csv_bucket_name, csv, local_csv_file)
                csv_files.append(local_csv_file)
            except Exception as e:
                raise AirflowFailException(f"Failed to download csv file: {csv_bucket_name}/{csv}: {e}")

    if not csv_files:
        raise AirflowFailException(f'No csv files found in: {csv_bucket_name}/{csv_dir_key}')

    # Combine CSV files into a single dataframe
    try:
        df = pd.concat([pd.read_csv(file) for file in csv_files], ignore_index=True)
    except Exception as e:
        raise AirflowFailException(f"Failed to combine csv files into single df: {e}")

    # Set the header if not provided
    if header is None:
        header = df.columns

    # Save the output to xlsx format
    local_excel_file = os.path.join(local_excel_directory, os.path.basename(excel_output_key))
    try:
        df.to_excel(local_excel_file, index=False, header=header, sheet_name=sheet_name)
    except Exception as e:
        raise AirflowFailException(f"Failed to convert {local_excel_file} to excel: {e}")

    # Upload to minio
    try:
        s3.load_file(local_excel_file, key=excel_output_key, bucket_name=excel_bucket_name, replace=True)
    except Exception as e:
        raise AirflowFailException(f"Failed to upload Excel file {local_excel_file} to bucket {excel_bucket_name}: {e}")