import kubernetes
from kubernetes.client import models as k8s
import logging
from airflow.exceptions import AirflowFailException
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator


class SparkOperator(KubernetesPodOperator):
    def __init__(
            self,
            spark_class: str,
            spark_jar: str,
            namespace: str,
            spark_config: str = '',
            **kwargs,
    ) -> None:
        super().__init__(
            is_delete_operator_pod=False,
            image='ferlabcrsj/spark:3.3.1',
            service_account_name='spark',
            retries=1,
            retry_delay=10,
            priority_weight=1,
            weight_rule="absolute",
            **kwargs
        )
        self.spark_class = spark_class
        self.spark_jar = spark_jar
        self.spark_config = spark_config
        self.namespace = namespace

    def execute(self, **kwargs):

        self.cmds = ['/opt/client-entrypoint.sh']
        self.image_pull_policy = 'IfNotPresent'

        self.env_vars = [
            k8s.V1EnvVar(
                name='SPARK_CLIENT_POD_NAME',
                value_from=k8s.V1EnvVarSource(
                    field_ref=k8s.V1ObjectFieldSelector(
                        field_path='metadata.name',
                    ),
                ),
            ),
            k8s.V1EnvVar(
                name='SPARK_JAR',
                value=self.spark_jar,
            ),
            k8s.V1EnvVar(
                name='SPARK_CLASS',
                value=self.spark_class,
            ),
        ]

        self.volumes = [
            k8s.V1Volume(
                name='spark-defaults',
                config_map=k8s.V1ConfigMapVolumeSource(
                    name='spark-defaults',
                ),
            ),
            k8s.V1Volume(
                name='spark-s3-credentials',
                secret=k8s.V1SecretVolumeSource(
                    secret_name='spark-s3-credentials',
                ),
            ),
        ]
        self.volume_mounts = [
            k8s.V1VolumeMount(
                name='spark-defaults',
                mount_path='/opt/spark-configs/defaults',
                read_only=True,
            ),
            k8s.V1VolumeMount(
                name='spark-s3-credentials',
                mount_path='/opt/spark-configs/s3-credentials',
                read_only=True,
            ),
        ]

        if self.spark_config:
            self.volumes.append(
                k8s.V1Volume(
                    name=self.spark_config,
                    config_map=k8s.V1ConfigMapVolumeSource(
                        name=self.spark_config,
                    ),
                ),
            )
            self.volume_mounts.append(
                k8s.V1VolumeMount(
                    name=self.spark_config,
                    mount_path=f'/opt/spark-configs/{self.spark_config}',
                    read_only=True,
                ),
            )

        super().execute(**kwargs)

        kubernetes.config.load_incluster_config()

        k8s_client = kubernetes.client.CoreV1Api()

        # Get driver pod log and delete driver pod
        driver_pod = k8s_client.list_namespaced_pod(
            namespace=self.pod.metadata.namespace,
            field_selector=f'metadata.name={self.pod.metadata.name}-driver',
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

