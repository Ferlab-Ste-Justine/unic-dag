import logging
from datetime import datetime
import os
import re

import pandas as pd
import psycopg2
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import DagRun
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from lib.hooks.postgresca import PostgresCaHook
from lib.postgres import get_pg_ca_hook
from lib.config import PUBLISHED_BUCKET, GREEN_MINIO_CONN_ID, YELLOW_MINIO_CONN_ID
from lib.tasks.excel import parquet_to_excel
from lib.publish_utils import FileType, add_extension_to_path

from sql.publish import update_dict_current_version_query, get_to_be_published_query, resource_query, dict_table_query, variable_query, value_set_query, value_set_code_query, mapping_query


@task
def get_resource_code(ti=None) -> str:
    dag_run: DagRun = ti.dag_run
    resource_code = dag_run.conf['resource_code']

    if not resource_code:
        raise AirflowFailException("DAG param 'resource_code' is required.")
    else:
        return resource_code

@task
def get_version_to_publish(ti=None) -> str:
    dag_run: DagRun = ti.dag_run
    version_to_publish = dag_run.conf['version_to_publish']
    date_format = "%Y-%m-%d"
    if not version_to_publish:
        raise AirflowFailException(f"DAG param 'version_to_publish' is required. Expected format: YYYY-MM-DD")
    elif bool(datetime.strptime(version_to_publish, date_format)):
        return dag_run.conf['version_to_publish']
    else:
        raise AirflowFailException(f"DAG param 'version_to_publish' is not in the correct format. Expected format: YYYY-MM-DD")

@task
def get_include_dictionary(ti=None) -> bool:
    dag_run: DagRun = ti.dag_run
    return dag_run.conf['include_dictionary']

@task
def get_release_id(ti=None) -> str:
    dag_run: DagRun = ti.dag_run
    release_id = dag_run.conf['release_id']
    regex = "^re_\d{4}$"
    if not release_id:
        return release_id
    elif re.fullmatch(regex, release_id):
        return dag_run.conf['release_id']
    else:
        raise AirflowFailException(f"DAG param 'release_id' is not in the correct format. Expected format: re_xxxx where x is a digit.")


@task.virtualenv(requirements=["pyhocon==0.3.61"], system_site_packages=True)
def extract_config_info(
        resource_code: str,
        version_to_publish: str,
        minio_conn_id: str = None,
        bucket: str = None,

) -> dict:
    """
    This function retrieves the necessary table names, S3 paths WITHOUT their extension,
    and bucket IDs for publishing data. The structure of the return dictionary is as follows:
    {
        "sources": {
            "source_id1": {
                "output_bucket": <bucket_name>,
                "output_path": <s3_path>,
                "table": <table_name>
            },
            "source_id2": {
                ...,
            }
        }
        ...,
        "has_clinical": bool - Indicates if the project has some table published in the clinical bucket. In that
        case, the dictionary will go in the clinical bucket. Otherwise, it will go in the nominative one.
        "has_nominative": bool - Indicates if the project has some table published in the nominative bucket.
        "input_bucket": <bucket_name> - The bucket from which the data will be published.

    }

    :param resource_code: Resource code of the project to publish.
    :param version_to_publish: Version of the project to publish.
    :param minio_conn_id: Minio connection id, defaults to YELLOW_MINIO_CONN_ID.
    :param bucket: S3 bucket from which the data will be published, defaults to PUBLISHED_BUCKET.
    :returns : Dictionary containing the source IDs, input & output buckets, output paths, and table names.

    """

    from lib.hocon_parsing import parse_hocon_conf, get_bucket_id, get_dataset_published_path
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from lib.config import YELLOW_MINIO_CONN_ID, PUBLISHED_BUCKET
    from lib.publish_utils import print_extracted_config

    # Set default constants if not provided
    if minio_conn_id is None:
        minio_conn_id = YELLOW_MINIO_CONN_ID
    if bucket is None:
        bucket = PUBLISHED_BUCKET


    # Initialize the mini_config
    mini_config = {}
    mini_config["has_clinical"] = False
    mini_config["has_nominative"] = False
    mini_config["sources"] = {}
    #Set the input bucket
    mini_config["input_bucket"] = bucket

    s3 = S3Hook(aws_conn_id=minio_conn_id)

    released_path = f"released/{resource_code}/{version_to_publish}/"

    config = parse_hocon_conf()

    table_paths = s3.list_prefixes(bucket, released_path, "/")
    for table_path in table_paths:
        table = table_path.split("/")[-2]  # Extract table name from the path, assumes the path structure is consistent

        #Reconstructing the source id of the table
        source_id = f"published_{resource_code}_{table}"

        output_bucket = get_bucket_id(source_id=source_id, config=config)

        if "clinical" in output_bucket:
            mini_config["has_clinical"] = True

        if "nominative" in output_bucket:
            mini_config["has_nominative"] = True

        output_path = get_dataset_published_path(source_id = source_id, config=config).replace("{{version}}", version_to_publish)

        mini_config["sources"][source_id] = {
            "output_bucket": output_bucket,
            "output_path": output_path,
            "table": table,
            "minio_conn_id": minio_conn_id
        }
    # Uncomment to print the extracted configuration
    #print_extracted_config(resource_code, version_to_publish, mini_config)
    return mini_config


@task(task_id="publish_dictionary",)
def publish_dictionary(
        resource_code: str,
        version_to_publish: str,
        include_dictionary: bool,
        pg_conn_id: str,
        config: dict,
        minio_conn_id: str = YELLOW_MINIO_CONN_ID) -> None:
    """
    Publish research project dictionary.

    :param resource_code: resource code of project to publish.
    :param version_to_publish: version of project to publish.
    :param include_dictionary: Specify if the dictionary should be included.
    :param pg_conn_id: Postgres connection id.
    :param config: Relevant info of this resource_code extracted from the hocon config and input minio path.
    :param minio_conn_id: Minio connection id.
    :return: None
    """

    if not include_dictionary:
        raise AirflowSkipException()

    ## Retrieve the bucket list from the config
    clinical_bucket_name = f"published-clinical-{resource_code}"
    nominative_bucket_name = f"published-nominative-{resource_code}"

    s3_destination_bucket = None

    if config["has_clinical"]:
        s3_destination_bucket = clinical_bucket_name
    if (not config["has_clinical"]) and config["has_nominative"] :
        s3_destination_bucket = nominative_bucket_name

    # At least one bucket must be present always
    if not s3_destination_bucket:
        raise AirflowFailException(f"The project {resource_code} does not have any table published in clinical or nominative buckets. Please check the config file.")

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

    # set up local Excel file
    local_excel_directory = '/tmp/excel/'
    os.makedirs(local_excel_directory, exist_ok=True)
    local_excel_file = f"{os.path.join(local_excel_directory, os.path.basename(resource_code))}.xlsx"

    # convert to excel
    try:
        with pd.ExcelWriter(local_excel_file) as excel_writer:
            for sheet, data in result_map.items():
                data.to_excel(excel_writer, sheet_name=sheet, index=False)
    except Exception as e:
        logging.error(f"Failed to convert {local_excel_file} to excel: {e}")
        raise AirflowFailException()

    # Upload to minio
    try:
        key = f"{version_to_publish}/{resource_code}_dictionary_{version_to_publish.replace('-', '_')}.xlsx"
        s3.load_file(local_excel_file, key=key, bucket_name=s3_destination_bucket, replace=True)
    except Exception as e:
        logging.error(f"Failed to upload Excel file {local_excel_file} to minio: {e}")
        raise AirflowFailException()


@task(task_id='update_dict_current_version')
def update_dict_current_version(dict_version: str, resource_code: str, include_dictionary: bool, pg_conn_id: str) -> None:
    """
    Update dict version for a given resource.

    :param dict_version: New dict version to set.
    :param resource_code: Resource code associated to the dict.
    :param include_dictionary: Specify if the dictionary should be included.
    :param pg_conn_id: Postgres connection id.
    :return: None
    """
    if not include_dictionary:
        raise AirflowSkipException()

    pg_conn = get_pg_ca_hook(pg_conn_id).get_conn()

    with pg_conn.cursor() as cur:
        try:
            cur.execute(update_dict_current_version_query(resource_code, dict_version))
            pg_conn.commit()
        except psycopg2.DatabaseError as e:
            pg_conn.rollback()
            logging.error(f"Failed to update dict version for {resource_code}: {e}")
            raise AirflowFailException()


@task
def set_publish_kwargs(resource_code: str, version_to_publish: str, config: dict) -> list:
    """
    Get kwargs for publishing data to Excel.

    :param resource_code: Resource code of the project to publish.
    :param version_to_publish: Version of the project to publish.
    :param config: Parsed HOCON configuration.
    :return: List of kwargs for publishing data.
    """

    list_of_kwargs = []
    for (source_id, source_info) in config["sources"].items():
        table = source_info["table"]

        list_of_kwargs.append({
            "parquet_bucket_name": config["input_bucket"],
            "parquet_dir_key": f'released/{resource_code}/{version_to_publish}/{table}',
            "excel_bucket_name": source_info["output_bucket"],
            "excel_output_key": add_extension_to_path(source_info["output_path"], FileType.EXCEL),
            "minio_conn_id": source_info["minio_conn_id"],
        })

    return list_of_kwargs

def get_to_be_published(resource_code: str, pg_conn_id: str) -> bool:
    """
    Get value of to_be_published for given resource code.

    :param resource_code: Resource code associated to the dict.
    :param pg_conn_id: Postgres connection id.
    :return: Boolean indicating if the resource is to be published.
    """

    pg_conn = get_pg_ca_hook(pg_conn_id).get_conn()

    with pg_conn.cursor() as cur:
        try:
            cur.execute(get_to_be_published_query(resource_code))
            return cur.fetchone()[0]
        except Exception as e:
            logging.error(f"Failed to retrive to_be_published for {resource_code}: {e}")
            raise AirflowFailException()

def validate_to_be_published(resource_code: str, pg_conn_id) -> ShortCircuitOperator:
    """
    Validate if the resource is to be published.

    :param resource_code: Resource code associated to the dict.
    :param pg_conn_id: Postgres connection id.
    :return: ShortCircuitOperator.
    """

    return ShortCircuitOperator(
        task_id="validate_to_be_published",
        python_callable=get_to_be_published,
        op_args=[resource_code, pg_conn_id],
    )