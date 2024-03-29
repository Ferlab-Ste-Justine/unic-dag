import kubernetes
import logging
from airflow.exceptions import AirflowFailException

class Cleanup:

    @staticmethod
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
        except Exception as e:
            logging.error(f'Failed to load in-cluster config: {e}')

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
            logging.info(f'Spark job log:\n{log}')
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

