import logging
import os

import requests
import pandas as pd

from airflow import DAG
from typing import List

from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from lib.operators.spark import SparkOperator
from lib.operators.spark_opensearch import SparkOpenSearchOperator
from airflow.exceptions import AirflowSkipException, AirflowFailException
from lib.config import os_url

from lib.hooks.postgresca import PostgresCaHook



def prepare_index(task_id: str, args: List[str], jar: str, spark_failure_msg: str, cluster_size: str,
                  dag: DAG, zone: str = "yellow",
                  spark_class: str = 'bio.ferlab.ui.etl.catalog.es.PrepareIndex') -> SparkOperator:

    return SparkOperator(
        task_id=task_id,
        name=task_id.replace("_", "-"),
        zone=zone,
        arguments=args,
        spark_class=spark_class,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config=cluster_size,
        dag=dag
    )

def load_index(task_id: str, args: List[str], jar: str, spark_failure_msg: str, cluster_size: str,dag: DAG,
          zone: str = "yellow", spark_class: str = 'bio.ferlab.ui.etl.catalog.os.index.Main') -> SparkOperator:

    return SparkOperator(
        task_id=task_id,
        name=task_id.replace("_", "-"),
        zone=zone,
        arguments=args,
        spark_class=spark_class,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config=cluster_size,
        dag=dag
    )

def publish_index(task_id: str, args: List[str], jar: str, spark_failure_msg: str, cluster_size: str,dag: DAG,
               zone: str = "yellow", spark_class: str = 'bio.ferlab.ui.etl.catalog.os.publish.Main') -> SparkOperator:

    return SparkOpenSearchOperator(
        task_id=task_id,
        name=task_id.replace("_", "-"),
        zone=zone,
        arguments=args,
        spark_class=spark_class,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config=cluster_size,
        dag=dag
    )

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

@task(task_id='get_release_id') # ne va pas marcher dans unic, le service est dans l'autre cluster.
def get_release_id(release_id: str, index: str, increment: bool = True, skip: bool = False) -> str:
    if skip:
        raise AirflowSkipException()

    if release_id:
        logging.info(f'Using release id passed to DAG: {release_id}')
        return release_id

    logging.info(f'No release id passed to DAG. Fetching release id from ES for all index {index}.')
    # Fetch current id from ES
    url = f'{os_url}/{index}?&pretty'
    response = requests.get(url)
    logging.info(f'ES response:\n{response.text}')

    # Parse current id
    current_full_release_id = list(response.json())[0]  # {index}_re_00xx
    current_release_id = current_full_release_id.split('_')[-1]  # 00xx
    logging.info(f'Current release id: re_{current_release_id}')

    if increment:
        # Increment current id by 1
        new_release_id = f're_{str(int(current_release_id) + 1).zfill(4)}'
        logging.info(f'New release id: {new_release_id}')
        return new_release_id
    else:
        return f're_{current_release_id}'
