import kubernetes
import logging
from airflow.exceptions import AirflowFailException

class Cleanup:

    def cleanup_pods(name, namespace, is_failure=False):
        """
        cleanup kubernetes pods
        :param name: pod name
        :param namespace: pod namespace
        :param is_failure: True if cleanup after job failure.
        :return:
        """
        # kubernetes.config.load_kube_config(
        #     config_file='~/.kube/config',
        #     context="unic-prod",
        # )

        kubernetes.config.load_incluster_config()

        k8s_client = kubernetes.client.CoreV1Api()

        driver_pod = k8s_client.list_namespaced_pod(
            namespace=namespace,
            field_selector=f'metadata.name={name}-driver',
            limit=1,
        )

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

        if not is_failure and driver_pod.items[0].status.phase != 'Succeeded':
            raise AirflowFailException('Spark job failed')

