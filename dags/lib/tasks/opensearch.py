import logging
import subprocess

from airflow import DAG
from typing import List

from airflow.decorators import task
from lib.operators.spark import SparkOperator
from lib.operators.spark_opensearch import SparkOpenSearchOperator
from lib.opensearch import (OpensearchEnv, os_credentials_username_key, os_credentials_password_key, os_prod_credentials_secret,
                            os_prod_cert_secret, os_qa_credentials_secret, os_qa_cert_secret, os_env_config, os_port,
                            os_prod_cert_path, os_cert_filename, os_prod_cert, os_qa_cert_path, os_qa_cert)


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

@task.virtualenv(
    task_id="publish_index", requirements=["opensearch-py==2.8.0"], system_site_packages=False
)
def publish_index(env_name: str, release_id: str, alias: str) -> None:
    from opensearchpy import OpenSearch
    from lib.opensearch import os_env_config

    os_config = os_env_config.get(env_name)

    # Load the os ca-certificate into task
    subprocess.run(["mkdir", "-p", os_config.get('ca_path')])

    with open(os_config.get('ca_path') + os_cert_filename, "w") as outfile:
        outfile.write(os_config.get('ca_cert'))

    # Create the OpenSearch client
    host = os_config.get('url')
    auth = (os_config.get('username'), os_config.get('password'))
    ca_certs_path = os_config.get('ca_path')

    os_client = OpenSearch(
        hosts = [{'host': host, 'port': os_port}],
        http_compress = True,
        http_auth = auth,
        use_ssl = True,
        verify_certs = True,
        ssl_assert_hostname = False,
        ssl_show_warn = False,
        ca_certs = ca_certs_path
    )

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
    logging.info("Alias updated:", response)

@task.virtualenv(
    task_id="get_next_release_id", requirements=["opensearch-py==2.8.0"]
)
def get_next_release_id(env_name: str, release_id: str, alias: str = 'resource_centric', increment: bool = True) -> str:
    """
    Get release id for openseach index.

    :param env_name: OpenSearch environment name (e.g. 'prod', 'qa')
    :param release_id: Release ID to use. If not provided, the current release ID will be fetched from OpenSearch and incrimented by 1.
    :param alias: Specify alias of OpenSearch index to get release_id from. Default is 'resource_centric'.
    :param increment: Specify weather to increment release_id. Default is True.
    :return: The next release_id
    """
    import logging

    from opensearchpy import OpenSearch
    from lib.opensearch import os_env_config

    os_config = os_env_config.get(env_name)

    # Load the os ca-certificate into task
    subprocess.run(["mkdir", "-p", os_config.get('ca_path')])

    with open(os_config.get('ca_path') + os_cert_filename, "w") as outfile:
        outfile.write(os_config.get('ca_cert'))

    # Create the OpenSearch client
    host = os_config.get('url')
    auth = (os_config.get('username'), os_config.get('password'))
    ca_certs_path = os_config.get('ca_path')

    os_client = OpenSearch(
        hosts = [{'host': host, 'port': os_port}],
        http_compress = True,
        http_auth = auth,
        use_ssl = True,
        verify_certs = True,
        ssl_assert_hostname = False,
        ssl_show_warn = False,
        ca_certs = ca_certs_path
    )

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


