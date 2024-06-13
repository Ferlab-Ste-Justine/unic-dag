import os
import pandas as pd
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def create_csv_to_excel_task(task_id, csv_bucket_name, csv_dir_key, excel_bucket_name, excel_dir_key, minio_conn_id='minio_conn_id'):
    """
    Create and Airflow task to convert multiple or single csv from a specified directory from Minio.
    Each ouput file is stored in another specified directory.

    Parms:
    - task_id:              name the Airflow task.
    - csv_bucket_name:      bucket where the CSV files are stored.
    - csv_dir_key:          directory prefix inside the bucket where csv files are located.
    - excel_bucket_name:    bucket where the output excel files will be stored.
    - excel_dir_key:        directory prefix where the output excel file(s) will be stored.
    - minio_conn_id:        connection id service, default -> 'minio_conn_id'.

    Returns:
    - function: An Airflow task to be used in DAGs for converting csv to excel.
    """
    
    @task(task_id=task_id)
    def csv_to_excel():
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

        for csv in keys:
            if csv.endswith('.csv'):
                local_csv_file = os.path.join(local_csv_directory, os.path.basename(csv))
                local_excel_file = os.path.join(local_excel_directory, os.path.basename(csv).replace('.csv', '.xlsx'))

                # Download the csv file
                try:
                    s3_client.download_file(csv_bucket_name, csv, local_csv_file)
                except Exception as e:
                    raise AirflowFailException(f"Failed to download csv file: {csv_bucket_name}/{csv}: {e}")

                # Save the output to xlsx format
                try:
                    df = pd.read_csv(local_csv_file)
                    df.to_excel(local_excel_file, index=False)
                except Exception as e:
                    raise AirflowFailException(f"Failed to convert csv to Excel: {e}")
                
                # Upload to minio
                try:
                    excel_key = os.path.join(excel_dir_key, os.path.basename(local_excel_file))
                    s3.load_file(local_excel_file, key=excel_key, bucket_name=excel_bucket_name, replace=True)
                except Exception as e:
                    raise AirflowFailException(f"Failed to upload Excel file: {excel_bucket_name}/{local_excel_file}: {e}")

    return csv_to_excel()