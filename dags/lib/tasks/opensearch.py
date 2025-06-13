from airflow import DAG
from typing import List

from airflow.decorators import task
from lib.operators.spark import SparkOperator

from lib.config import CATALOG_ZONE, CATALOG_BUCKET


def prepare_index(task_id: str,
                  args: List[str],
                  jar: str,
                  spark_failure_msg: str,
                  cluster_size: str,
                  dag: DAG,
                  zone: str = CATALOG_ZONE,
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
def load_index(env_name: str, release_id: str, alias: str, src_path: str) -> None:

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

    from lib.opensearch import load_cert, get_opensearch_client, OS_TEMPLATES, OS_ID_COLUMNS
    from lib.config import YELLOW_MINIO_CONN_ID, CATALOG_BUCKET


    # Load the os ca-certificate into task
    load_cert(env_name)

    # Get OpenSearch client
    os_client = get_opensearch_client(env_name)

    # Get s3 client
    s3 = S3Hook(aws_conn_id=YELLOW_MINIO_CONN_ID)

    index_name = f"{alias}_{release_id}"
    template_name = f"{alias}_template"

    # List all the files for a given dir in Minio
    try:
        # get index key from minio
        keys = s3.list_keys(bucket_name=CATALOG_BUCKET, prefix=f"{src_path}{alias}/")
        if not keys:
            logging.error(f"No files found in: {CATALOG_BUCKET}/{src_path}")
            raise AirflowFailException()
    except Exception as e:
        logging.error(f"Failed to list the files from: {CATALOG_BUCKET}/{src_path}: {e}")
        raise AirflowFailException()

    # get index data from minio
    parquet_files = []
    for key in keys:
        if key.endswith('.parquet'):
            try:
                s3_response = s3.get_key(key=key, bucket_name=CATALOG_BUCKET)
                parquet_files.append(s3_response.get()['Body'].read())
            except Exception as e:
                logging.error(f"Failed to download the file: {CATALOG_BUCKET}/{key}: {e}")
                raise AirflowFailException()

    try:
        df = pd.concat([pd.read_parquet(BytesIO(file)) for file in parquet_files], ignore_index=True)
    except Exception as e:
        logging.error(f"Failed to combine parquet files into single df: {e}")
        raise AirflowFailException()

    try:
        # delete index if already exists
        os_client.indices.delete(index=index_name, ignore=404)
        logging.info(f"Deleted index: {index_name}")

        # load template
        os_client.indices.put_index_template(name=template_name, body=OS_TEMPLATES.get(alias))
        logging.info(f"Loaded template: {template_name}")

        # load index data
        json_data = json.loads(df.to_json(orient='records'))

        chunk_size = 50000
        split_json_data = [json_data[i:i+chunk_size] for i in range(0,len(json_data),chunk_size)]

        for chunk in split_json_data:
            data = []
            for record in chunk:
                data.append({"index": {"_index": index_name, "_id": record.get(OS_ID_COLUMNS.get(alias))}})
                data.append(record)

            bulk_response = os_client.bulk(data)

            if bulk_response['errors']:
                logging.error(f"Errors occurred during bulk insert: {bulk_response}")
                raise AirflowFailException()

            logging.info(f"Bulk-inserted {len(bulk_response['items'])} items.")

    except Exception as e:
        logging.error(f"Failed to load index in Opensearch: {e}")
        raise AirflowFailException()

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
    from lib.opensearch import MAX_RELEASE_ID_NUM, NUM_VERSIONS_TO_KEEP, load_cert, get_opensearch_client
    from airflow.exceptions import AirflowFailException

    # Load the os ca-certificate into task
    load_cert(env_name)

    # Get OpenSearch client
    os_client = get_opensearch_client(env_name)

    new_index = f"{alias}_{release_id}"

    alias_exists = os_client.indices.exists_alias(name=alias)
    logging.info(f"Alias '{alias}' exists: {alias_exists}")

    actions = []
    if alias_exists:
        alias_info = os_client.indices.get_alias(name=alias)
        current_index = next(iter(alias_info), "")
        actions.append({"remove": {"index": current_index, "alias": alias}})
    else:
        current_index = "No alias"

    actions.append({"add": {"index": new_index, "alias": alias}})

    logging.info(f"Current Index: {current_index}")
    logging.info(f"New Index: {new_index}")

    try:
        response = os_client.indices.update_aliases(body={"actions": actions})
        logging.info(f"Alias updated: {response}")
    except Exception as e:
        logging.error(f"Failed to update Alias in Opensearch: {e}")
        raise AirflowFailException()

    # Delete old index, keep 5 most recent
    current_release_id_num = int(release_id.split('_')[-1])
    if current_release_id_num < NUM_VERSIONS_TO_KEEP:
        release_id_num_to_delete = (MAX_RELEASE_ID_NUM + 1) - (NUM_VERSIONS_TO_KEEP - current_release_id_num)
    else:
        release_id_num_to_delete = current_release_id_num - NUM_VERSIONS_TO_KEEP

    release_id_to_delete = f're_{str(release_id_num_to_delete).zfill(4)}'
    index_to_delete = f"{alias}_{release_id_to_delete}"
    try:
        if os_client.indices.exists(index=index_to_delete):
            response = os_client.indices.delete(index=index_to_delete)
            logging.info(f"Deleted index {index_to_delete}: {response}")
        else:
            logging.info(f"Index {index_to_delete} does not exist, skipping deletion.")
    except Exception as e:
        logging.error(f"Failed to delete index {index_to_delete} from Opensearch: {e}")
        raise AirflowFailException()

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
    from lib.opensearch import OS_ENV_CONFIG, MAX_RELEASE_ID_NUM, MIN_RELEASE_ID_NUM, load_cert, get_opensearch_client
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
        current_release_id_num = int(current_index.split('_')[-1])
    except Exception as e:
        logging.error(f"Failed to retrieve current release id from Opensearch: {e}")
        raise AirflowFailException()

    if increment:
        # Increment current id by 1
        if current_release_id_num == MAX_RELEASE_ID_NUM:
            return f're_{str(MIN_RELEASE_ID_NUM).zfill(4)}'

        new_release_id = f're_{str(current_release_id_num + 1).zfill(4)}'
        logging.info(f'New release id: {new_release_id}')
        return new_release_id
    else:
        return f're_{str(current_release_id_num)}'


