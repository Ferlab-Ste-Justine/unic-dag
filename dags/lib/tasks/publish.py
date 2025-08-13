import logging
from datetime import datetime
import os
import re

import pandas as pd
import psycopg2
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import DagRun
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from lib.hooks.postgresca import PostgresCaHook
from lib.postgres import get_pg_ca_hook, PostgresEnv
from lib.config import PUBLISHED_BUCKET, GREEN_MINIO_CONN_ID, YELLOW_MINIO_CONN_ID, DEFAULT_VERSION
from lib.tasks.excel import parquet_to_excel
from lib.publish_utils import FileType, add_extension_to_path, choose_minio_conn_id

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
    elif version_to_publish == DEFAULT_VERSION or bool(datetime.strptime(version_to_publish, date_format)):
        return dag_run.conf['version_to_publish']
    else:
        raise AirflowFailException(f"DAG param 'version_to_publish' is not in the correct format. Expected format: YYYY-MM-DD")

@task
def get_include_dictionary(ti=None) -> bool:
    dag_run: DagRun = ti.dag_run
    return dag_run.conf['include_dictionary']


@task
def get_skip_index(ti=None) -> bool:
    dag_run: DagRun = ti.dag_run
    return dag_run.conf['skip_index']


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
        minio_conn_id: str = None
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
        "clinical_bucket": str - Name of the clinical bucket, if it exists, otherwise None.
        "nominative_bucket": str - Name of the nominative bucket, if it exists, otherwise None.
        "input_bucket": <bucket_name> - The bucket from which the data will be published.

    }

    :param resource_code: Resource code of the project to publish.
    :param version_to_publish: Version of the project to publish.
    :param minio_conn_id: Minio connection id, defaults to YELLOW_MINIO_CONN_ID.
    :returns : Dictionary containing the source IDs, input & output buckets, output paths, and table names.

    """

    from lib.hocon_parsing import parse_hocon_conf, get_bucket_name, get_dataset_published_path, get_released_bucket_name
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from lib.config import YELLOW_MINIO_CONN_ID, PUBLISHED_BUCKET
    from lib.publish_utils import print_extracted_config, choose_minio_conn_id

    config = parse_hocon_conf()

    # Set default constants if not provided
    if minio_conn_id is None:
        minio_conn_id = YELLOW_MINIO_CONN_ID

    # Initialize the mini_config
    mini_config = {}
    mini_config["clinical_bucket"] = None
    mini_config["nominative_bucket"] = None
    mini_config["sources"] = {}
    # Set the input bucket
    input_bucket = get_released_bucket_name(resource_code=resource_code, config=config)
    mini_config["input_bucket"] = input_bucket

    chosen_conn_id = choose_minio_conn_id(config=mini_config, minio_conn_id=minio_conn_id)
    s3 = S3Hook(aws_conn_id=chosen_conn_id)

    released_path = f"released/{resource_code}/{version_to_publish}/"

    table_paths = s3.list_prefixes(input_bucket, released_path, "/")
    for table_path in table_paths:
        table = table_path.split("/")[-2]  # Extract table name from the path, assumes the path structure is consistent

        # Reconstructing the source id of the table
        source_id = f"published_{resource_code}_{table}"

        output_bucket = get_bucket_name(source_id=source_id, config=config)

        if "clinical" in output_bucket:
            mini_config["clinical_bucket"] = output_bucket

        if "nominative" in output_bucket:
            mini_config["nominative_bucket"] = output_bucket

        output_path = get_dataset_published_path(source_id=source_id, config=config)
        # Replacing the version in the filename with the underscore version to publish
        output_path = output_path.replace("_{{version}}", f"_{version_to_publish.replace('-','_')}")
        # Replacing the version template for the folder with the dashed version to publish
        output_path = output_path.replace("{{version}}", version_to_publish)

        # If the output bucket is the default green bucket, we must place the output path in the published folder
        if output_bucket == PUBLISHED_BUCKET:
            output_path = f"published{output_path}"

        mini_config["sources"][source_id] = {
            "output_bucket": output_bucket,
            "output_path": output_path,
            "table": table,
        }
    # Uncomment to print the extracted configuration
    # print_extracted_config(resource_code, version_to_publish, mini_config)
    return mini_config


def get_dictionary_output_bucket_name(clinical_bucket: str,
                                      nominative_bucket: str) -> str:
    """
    Get the output bucket name for the dictionary based on the outputs of the mini-config.

    :param clinical_bucket: The clinical bucket name, if it exists.
    :param nominative_bucket: The nominative bucket name, if it exists.
    :return: The output bucket name for the dictionary.
    """
    if clinical_bucket is not None:
        return clinical_bucket
    elif nominative_bucket is not None:
        return nominative_bucket
    else:
        return PUBLISHED_BUCKET


@task(task_id="publish_dictionary")
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
    :param s3_destination_bucket: S3 bucket where the dictionary will be published.
    :param minio_conn_id: Minio connection id.
    :return: None
    """

    if not include_dictionary:
        raise AirflowSkipException()

    s3_destination_bucket = get_dictionary_output_bucket_name(
        clinical_bucket=config["clinical_bucket"],
        nominative_bucket=config["nominative_bucket"]
    )

    # define connection vars
    s3 = S3Hook(aws_conn_id=choose_minio_conn_id(config, minio_conn_id))
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
def get_publish_kwargs(resource_code: str, version_to_publish: str, minio_conn_id: str, config: dict) -> list:
    """
    Get kwargs for publishing data to Excel.

    :param resource_code: Resource code of the project to publish.
    :param version_to_publish: Version of the project to publish.
    :param minio_conn_id: Minio connection id, defaults to YELLOW_MINIO_CONN_ID.
    :param config: Parsed HOCON configuration.
    :return: List of kwargs for publishing data.
    """

    # Set default constants if not provided
    if minio_conn_id is None:
        minio_conn_id = YELLOW_MINIO_CONN_ID

    # Choose the appropriate conn id depending on the config
    chosen_conn_id = choose_minio_conn_id(config=config, minio_conn_id=minio_conn_id)

    list_of_kwargs = []
    for (source_id, source_info) in config["sources"].items():
        table = source_info["table"]

        list_of_kwargs.append({
            "parquet_bucket_name": config["input_bucket"],
            "parquet_dir_key": f'released/{resource_code}/{version_to_publish}/{table}',
            "excel_bucket_name": source_info["output_bucket"],
            "excel_output_key": add_extension_to_path(source_info["output_path"], FileType.EXCEL),
            "minio_conn_id": chosen_conn_id,
        })

    return list_of_kwargs

def trigger_publish_dag(
        resource_code: str,
        version_to_publish: str,
        include_dictionary: bool = True,
        skip_index: bool = True,
        release_id: str = "",
        env : PostgresEnv = PostgresEnv.PROD) -> TriggerDagRunOperator:
    """
    Trigger the publish DAG with the given parameters.

    :param resource_code: Resource code of the project to publish.
    :param version_to_publish: Version of the project to publish.
    :param include_dictionary: Specify if the dictionary should be included, True by default.
    :param skip_index: Boolean that sets whether the OpenSearch indexing is skipped, True by default.
    :param release_id: Release id of OpenSearch index.
    :param env: Postgres environment to use for the DAG, default of "prod"
    :return: TriggerDagRunOperator
    """
    return TriggerDagRunOperator(
        task_id = f"trigger_publish_{resource_code}",
        trigger_dag_id = f"unic_publish_project_{env.value}",
        conf = {
            "resource_code": resource_code,
            "version_to_publish": version_to_publish,
            "include_dictionary": include_dictionary,
            "skip_index": skip_index,
            "release_id": release_id
        },
        wait_for_completion = True, # Wait for the triggered DAG to complete before continuing
        poke_interval = 60, # Check the status of the triggered DAG every 60 seconds.
        failed_states = ["failed"], # The default failed_states is None, thus provide "failed" as a failed state to check against.
        retries = 3,  # Number of retries if the trigger fails
)


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


@task.short_circuit
def validate_to_be_published(resource_code: str, pg_conn_id, skip: bool = False) -> bool:
    """
    Validate if the resource is to be published.

    :param resource_code: Resource code associated to the dict.
    :param pg_conn_id: Postgres connection id.
    :param skip: If True, skips the task. False by default.
    :return: ShortCircuitOperator.
    """

    if skip:
        raise AirflowSkipException()

    return get_to_be_published(resource_code=resource_code, pg_conn_id=pg_conn_id)
