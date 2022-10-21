import kubernetes
import logging
from airflow.exceptions import AirflowFailException
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

class SparkOperator(KubernetesPodOperator):

    def execute(self, **kwargs):

        super().execute(**kwargs)

        k8s_client = kubernetes.client.CoreV1Api()

        # Get driver pod log and delete driver pod
        driver_pod = k8s_client.list_namespaced_pod(
            namespace=self.pod.metadata.namespace,
            field_selector=f'{self.pod.metadata.name}-driver',
            limit=1,
        )
        if driver_pod.items:
            log = k8s_client.read_namespaced_pod_log(
                name=f'{self.pod.metadata.name}-driver',
                namespace=self.pod.metadata.namespace,
            )
            logging.info(f'Spark job log:\n{log}')
            k8s_client.delete_namespaced_pod(
                name=f'{self.pod.metadata.name}-driver',
                namespace=self.pod.metadata.namespace,
            )

        # Delete pod
        pod = k8s_client.list_namespaced_pod(
            namespace=self.pod.metadata.namespace,
            field_selector=f'metadata.name={self.pod.metadata.name}',
            limit=1,
        )
        if pod.items:
            k8s_client.delete_namespaced_pod(
                name=self.pod.metadata.name,
                namespace=self.pod.metadata.namespace,
            )

        # Fail task if driver pod failed
        if driver_pod.items[0].status.phase != 'Succeeded':
            raise AirflowFailException('Spark job failed')
