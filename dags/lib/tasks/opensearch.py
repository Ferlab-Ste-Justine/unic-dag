import logging

import requests
import os

from airflow import DAG
from typing import List

from airflow.decorators import task
from lib.operators.spark import SparkOperator
from lib.operators.spark_opensearch import SparkOpenSearchOperator
from lib.operators.python_opensearch import PythonOpenSearchOperator
from lib.opensearch import (OpensearchEnv, os_credentials_username_key, os_credentials_password_key, os_prod_url, os_prod_credentials_secret,
                            os_prod_cert_secret, os_qa_credentials_secret, os_qa_cert_secret, os_prod_cert_path,
                            os_cert_filename, os_prod_cert, os_qa_cert_path, os_qa_cert, os_qa_username, os_qa_password, os_qa_cert_path, os_port)


def prepare_index(task_id: str, args: List[str], jar: str, spark_failure_msg: str, cluster_size: str,
                  dag: DAG, zone: str = "yellow",
                  spark_class: str = 'bio.ferlab.ui.etl.catalog.os.PrepareIndex') -> SparkOperator:

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

def publish_index(task_id: str, args: List[str], jar: str, spark_failure_msg: str, cluster_size: str, env_name: str,
                  dag: DAG, zone: str = "yellow", spark_class: str = 'bio.ferlab.ui.etl.catalog.os.publish.Main') -> SparkOperator:

    if env_name == OpensearchEnv.PROD.value:
        return SparkOpenSearchOperator(
            task_id=task_id,
            name=task_id.replace("_", "-"),
            zone=zone,
            arguments=args,
            spark_class=spark_class,
            spark_jar=jar,
            spark_failure_msg=spark_failure_msg,
            spark_config=cluster_size,
            os_cert_secret_name=os_prod_cert_secret,
            os_credentials_secret_name=os_prod_credentials_secret,
            os_credentials_secret_key_username=os_credentials_username_key,
            os_credentials_secret_key_password=os_credentials_password_key,
            dag=dag
        )
    elif env_name == OpensearchEnv.QA.value:
        return SparkOpenSearchOperator(
            task_id=task_id,
            name=task_id.replace("_", "-"),
            zone=zone,
            arguments=args,
            spark_class=spark_class,
            spark_jar=jar,
            spark_failure_msg=spark_failure_msg,
            spark_config=cluster_size,
            os_cert_secret_name=os_qa_cert_secret,
            os_credentials_secret_name=os_qa_credentials_secret,
            os_credentials_secret_key_username=os_credentials_username_key,
            os_credentials_secret_key_password=os_credentials_password_key,
            dag=dag
        )
    else:
        return None

def get_release_id_callable(release_id: str, index: str, increment: bool, **context) -> str:
    task_instance = context['task_instance']

    logging.info(f'RELEASE ID: {release_id}')
    if release_id:
        logging.info(f'Using release id passed to DAG: {release_id}')
        task_instance.xcom_push(key="release_id", value=release_id)



    logging.info(f'No release id passed to DAG. Fetching release id from OS for all index {index}.')
    # Fetch current id from OS
    url = f'{os_prod_url}:{os_port}/{index}?&pretty'
    response = requests.get(url, auth=(os_qa_username, os_qa_password), verify=os_qa_cert_path) # change to accept qa or prod
    logging.info(f'OS response:\n{response.text}')

    # Parse current id
    current_full_release_id = list(response.json())[0]  # {index}_re_00xx
    current_release_id = current_full_release_id.split('_')[-1]  # 00xx
    logging.info(f'Current release id: re_{current_release_id}')

    if increment:
        # Increment current id by 1
        new_release_id = f're_{str(int(current_release_id) + 1).zfill(4)}'
        logging.info(f'New release id: {new_release_id}')
        task_instance.xcom_push(key="release_id", value=new_release_id)
    else:
        task_instance.xcom_push(key="release_id", value=f're_{current_release_id}')

def get_release_id(task_id: str, env_name: str, release_id: str, index: str = 'resource_centric', increment: bool = True, skip: bool = False, **context) -> PythonOpenSearchOperator:
    if env_name == OpensearchEnv.PROD.value:
        return PythonOpenSearchOperator(
            task_id=task_id,
            python_callable=get_release_id_callable,
            op_kwargs={'release_id': release_id, 'index': index, 'increment': increment},
            ca_path = os_prod_cert_path,
            ca_filename = os_cert_filename,
            ca_cert = os_prod_cert,
            provide_context=True,
            skip=skip
        )

    elif env_name == OpensearchEnv.QA.value:
        return PythonOpenSearchOperator(
            task_id=task_id,
            python_callable=get_release_id_callable,
            op_kwargs={'release_id': release_id, 'index': index, 'increment': increment},
            ca_path = os_qa_cert_path,
            ca_filename = os_cert_filename,
            ca_cert = os_qa_cert,
            provide_context=True,
            skip=skip
        )
    else:
        return None
