from airflow import DAG
from typing import List

from airflow.decorators import task
from lib.operators.spark import SparkOperator

def prepare_index(task_id: str, args: List[str], jar: str, spark_failure_msg: str, cluster_size: str,
                  dag: DAG, zone: str = "yellow",
                  spark_class: str = 'bio.ferlab.ui.etl.catalog.os.prepare.Main') -> SparkOperator:

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

@task.virtualenv(
    task_id="load_index", requirements=["opensearch-py==2.8.0"]
)
def load_index(env_name: str, release_id: str, alias: str) -> None:

    """
    Publish index by updating alias.

    :param env_name: OpenSearch environment name (e.g. 'prod', 'qa')
    :param release_id: Release ID to use.
    :param alias: Specify alias of OpenSearch index to publish.
    :return: None
    """
    import logging
    import pandas as pd
    from io import BytesIO

    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    from lib.opensearch import load_cert, get_opensearch_client
    from lib.config import minio_conn_id


    # Load the os ca-certificate into task
    load_cert(env_name)

    # Get OpenSearch client
    os_client = get_opensearch_client(env_name)

    # Get s3 client
    s3 = S3Hook(aws_conn_id="minio")

    keys = s3.list_keys(bucket_name="yellow-prd", prefix="catalog/prod/os_index/resource_centric")
    logging.info(f"KEYS: {keys}")

    if len(keys) != 1:
        raise Exception(f"More than one index found for {alias} in S3.")
    else:
        index_key = keys[0]
        s3_response = s3.get_key(key=index_key, bucket_name="yellow-prd")

        parquet_bytes = s3_response.get()['Body'].read()

        index_df = pd.read_parquet(BytesIO(parquet_bytes)).to_dict('index')



@task.virtualenv(
    task_id="publish_index", requirements=["opensearch-py==2.8.0"]
)
def publish_index(env_name: str, release_id: str, alias: str) -> None:
    """
    Publish index by updating alias.

    :param env_name: OpenSearch environment name (e.g. 'prod', 'qa')
    :param release_id: Release ID to use.
    :param alias: Specify alias of OpenSearch index to publish.
    :return: None
    """
    import logging
    from lib.opensearch import load_cert, get_opensearch_client

    # Load the os ca-certificate into task
    load_cert(env_name)

    # Get OpenSearch client
    os_client = get_opensearch_client(env_name)

    new_index = f"{alias}_{release_id}"

    alias_info = os_client.indices.get_alias(name=alias)
    current_index = list(alias_info.keys())[0]

    logging.info(f"Current Index: {current_index}")
    logging.info(f"New Index: {new_index}")

    actions = [
        {"remove": {"index": current_index, "alias": alias}},
        {"add": {"index": new_index, "alias": alias}}
    ]

    response = os_client.indices.update_aliases(body={"actions": actions})
    logging.info(f"Alias updated: {response}")

@task.virtualenv(
    task_id="get_next_release_id", requirements=["opensearch-py==2.8.0"]
)
def get_next_release_id(env_name: str, release_id: str, alias: str = 'resource_centric', increment: bool = True) -> str:
    """
    Get release id for openseach index.

    :param env_name: OpenSearch environment name (e.g. 'prod', 'qa')
    :param release_id: Release ID to use. If not provided, the current release ID will be fetched from OpenSearch and incremented by 1.
    :param alias: Specify alias of OpenSearch index to get release_id from. Default is 'resource_centric'.
    :param increment: Specify whether to increment release_id. Default is True.
    :return: The next release_id
    """
    import logging
    from lib.opensearch import os_env_config, load_cert, get_opensearch_client

    # Load the os ca-certificate into task
    load_cert(env_name)

    # Get OpenSearch client
    os_client = get_opensearch_client(env_name)

    logging.info(f'RELEASE ID: {release_id}')

    if release_id:
        logging.info(f'Using release id passed to DAG: {release_id}')
        return release_id

    logging.info(f'No release id passed to DAG. Fetching release id from OS for all index {alias}.')
    # Fetch current id from OS
    alias_info = os_client.indices.get_alias(name=alias)
    current_index = list(alias_info.keys())[0]
    current_release_id = current_index.split('_')[-1]

    if increment:
        # Increment current id by 1
        new_release_id = f're_{str(int(current_release_id) + 1).zfill(4)}'
        logging.info(f'New release id: {new_release_id}')
        return new_release_id
    else:
        return f're_{current_release_id}'


