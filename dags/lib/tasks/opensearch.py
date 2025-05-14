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

def load_index_spark(task_id: str, args: List[str], jar: str, spark_failure_msg: str, cluster_size: str,dag: DAG,
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


@task.virtualenv(
    task_id="load_index", requirements=["opensearch-py==2.8.0"]
)
def load_index(env_name: str, release_id: str, alias: str, src_bucket: str = "yellow-prd", src_path: str = "catalog/prod/os_index/") -> None:

    """
    Load index in Opensearch.

    :param env_name: OpenSearch environment name (e.g. 'prod', 'qa')
    :param release_id: Release ID to use.
    :param alias: Specify alias of OpenSearch index to publish.
    :return: None
    """
    import logging
    import pandas as pd
    from io import BytesIO
    import json

    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from airflow.exceptions import AirflowFailException

    from lib.opensearch import load_cert, get_opensearch_client, os_templates, os_id_columns
    from lib.config import yellow_minio_conn_id


    # Load the os ca-certificate into task
    load_cert(env_name)

    # Get OpenSearch client
    os_client = get_opensearch_client(env_name)

    # Get s3 client
    s3 = S3Hook(aws_conn_id=yellow_minio_conn_id)

    index_name = f"{alias}_{release_id}"
    template_name = f"{alias}_template"

    # List all the files for a given dir in Minio
    try:
        # get index key from minio
        keys = s3.list_keys(bucket_name=src_bucket, prefix=f"{src_path}{alias}/")
        if not keys:
            raise AirflowFailException(f"No files found in: {src_bucket}/{src_path}")
    except Exception as e:
        raise AirflowFailException(f"Failed to list the files from: {src_bucket}/{src_path}: {e}")

    # get index data from minio
    parquet_files = []
    for key in keys:
        if key.endswith('.parquet'):
            try:
                s3_response = s3.get_key(key=key, bucket_name=src_bucket)
                parquet_files.append(s3_response.get()['Body'].read())
            except Exception as e:
                raise AirflowFailException(f"Failed to download the file: {src_bucket}/{key}: {e}")

    try:
        df = pd.concat([pd.read_parquet(BytesIO(file)) for file in parquet_files], ignore_index=True)
    except Exception as e:
        raise AirflowFailException(f"Failed to combine parquet files into single df: {e}")

    try:
        # delete index if already exists
        os_client.indices.delete(index=index_name, ignore=404)
        logging.info(f"Deleted index: {index_name}")

        # load template
        os_client.indices.put_index_template(name=template_name, body=os_templates.get(alias))
        logging.info(f"Loaded template: {template_name}")

        # load index data
        data = []
        for record in json.loads(df.to_json(orient='records')):
            data.append({"index": {"_index": index_name, "_id": record.get(os_id_columns.get(alias))}})
            data.append(record)

        bulk_response = os_client.bulk(data)
        logging.info(f"Bulk-inserted {len(bulk_response['items'])} items.")

    except Exception as e:
        raise AirflowFailException(f"Failed to load index in Opensearch: {e}")

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
    from airflow.exceptions import AirflowFailException

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

    try:
        response = os_client.indices.update_aliases(body={"actions": actions})
        logging.info(f"Alias updated: {response}")
    except Exception as e:
        raise AirflowFailException(f"Failed to update Alias in Opensearch: {e}")

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
    from airflow.exceptions import AirflowFailException

    # Load the os ca-certificate into task
    load_cert(env_name)

    # Get OpenSearch client
    os_client = get_opensearch_client(env_name)

    logging.info(f'RELEASE ID: {release_id}')

    if release_id:
        logging.info(f'Using release id passed to DAG: {release_id}')
        return release_id

    try:
        logging.info(f'No release id passed to DAG. Fetching release id from OS for all index {alias}.')
        # Fetch current id from OS
        alias_info = os_client.indices.get_alias(name=alias)
        current_index = list(alias_info.keys())[0]
        current_release_id = current_index.split('_')[-1]
    except Exception as e:
        raise AirflowFailException(f"Failed to retrieve current release id from Opensearch: {e}")

    if increment:
        # Increment current id by 1
        new_release_id = f're_{str(int(current_release_id) + 1).zfill(4)}'
        logging.info(f'New release id: {new_release_id}')
        return new_release_id
    else:
        return f're_{current_release_id}'


