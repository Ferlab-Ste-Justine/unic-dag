import logging

import kubernetes
from airflow.exceptions import AirflowFailException


def cleanup_pods(name, namespace, spark_failure_msg, failed=False):
    """
    Clean up the Spark pods launched by a task: log and delete the driver pod (its executors are
    garbage-collected via their owner reference to the driver) and delete the client pod.

    :param name: client pod name; the driver pod is expected to be named "{name}-driver"
    :param namespace: namespace the pods run in
    :param spark_failure_msg: message for the AirflowFailException raised when the driver did not succeed
    :param failed: True when cleaning up after a kill/failure, to skip raising on a non-succeeded driver
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
