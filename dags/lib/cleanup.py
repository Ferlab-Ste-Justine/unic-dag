import logging

import kubernetes
from airflow.exceptions import AirflowFailException


def cleanup_pods(name, namespace, spark_failure_msg, failed=False):
    """
    cleanup kubernetes pods
    :param name: pod name
    :param namespace: pod namespace
    :param is_failure: True if cleanup after job failure.
    :return:
    """
    try:
        kubernetes.config.load_incluster_config()
    except kubernetes.config.ConfigException as error:
        logging.error('Failed to load in-cluster config: %s', error)

    k8s_client = kubernetes.client.CoreV1Api()

    driver_pod = k8s_client.list_namespaced_pod(
        namespace=namespace,
        field_selector=f'metadata.name={name}-driver',
        limit=1,
    )

    # Print spark logs and delete driver pod
    if driver_pod.items:
        log = k8s_client.read_namespaced_pod_log(
            name=f'{name}-driver',
            namespace=namespace,
        )
        logging.info('Spark job log:\n%s', log)
        k8s_client.delete_namespaced_pod(
            name=f'{name}-driver',
            namespace=namespace
        )

    # Delete pod
    pod = k8s_client.list_namespaced_pod(
        namespace=namespace,
        field_selector=f'metadata.name={name}',
        limit=1,
    )
    if pod.items:
        k8s_client.delete_namespaced_pod(
            name=name,
            namespace=namespace
        )

    if not failed and driver_pod.items:
        if driver_pod.items[0].status.phase != 'Succeeded':
            raise AirflowFailException(spark_failure_msg)
