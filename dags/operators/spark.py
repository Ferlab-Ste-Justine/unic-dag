from kubernetes.client import models as k8s
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

from core.cleanup import Cleanup

class SparkOperator(KubernetesPodOperator):
    template_fields = KubernetesPodOperator.template_fields + (
        'spark_jar',
    )

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
            priority_weight=1,
            weight_rule="absolute",
            depends_on_past=False,
            **kwargs
        )
        self.spark_class = spark_class
        self.spark_jar = spark_jar
        self.namespace = namespace
        self.spark_config = spark_config

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

        Cleanup.cleanup_pods(self.pod.metadata.name, self.pod.metadata.namespace, self.spark_failure_msg)

