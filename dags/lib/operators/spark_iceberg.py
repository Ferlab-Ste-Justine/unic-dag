from airflow.exceptions import AirflowSkipException
from kubernetes.client import models as k8s

from lib.cleanup import Cleanup

from lib.operators.spark import SparkOperator


class SparkIcebergOperator(SparkOperator):
    template_fields = SparkOperator.template_fields + (
        'spark_class',
        'spark_jar',
        'spark_failure_msg',
        'zone',
        'spark_config',
        'skip'
    )

    def __init__(
            self,
            spark_class: str,
            spark_jar: str,
            spark_failure_msg: str,
            zone: str,
            spark_config: str = '',
            skip: bool = False,
            **kwargs,
    ) -> None:
        self.spark_class = spark_class
        self.spark_jar = spark_jar
        self.spark_failure_msg = spark_failure_msg
        self.zone = zone
        self.spark_config = spark_config
        self.skip = skip
        super().__init__(
            spark_class=spark_class,
            spark_jar=spark_jar,
            spark_failure_msg=spark_failure_msg,
            zone=zone,
            spark_config=spark_config,
            **kwargs
        )

    def execute(self, **kwargs):
        if self.skip:
            raise AirflowSkipException()

        new_volumes = [
            k8s.V1Volume(
                name='iceberg-s3-credentials',
                secret=k8s.V1SecretVolumeSource(
                    secret_name='iceberg-s3-credentials',
                ),
            ),
        ]

        if self.volumes:
            self.volumes += new_volumes
        else:
            self.volumes = new_volumes

        new_volume_mounts = [
            k8s.V1VolumeMount(
                name='iceberg-s3-credentials',
                mount_path='/opt/spark-configs/iceberg-credentials',
                read_only=True,
            )
        ]

        if self.volume_mounts:
            self.volume_mounts += new_volume_mounts
        else:
            self.volume_mounts = new_volume_mounts

        super().execute(**kwargs)

