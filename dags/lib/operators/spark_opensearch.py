import subprocess

from airflow.exceptions import AirflowSkipException

from lib.operators.spark import SparkOperator
from kubernetes.client import models as k8s
from typing import Optional


class SparkOpenSearchOperator(SparkOperator):
    template_fields = SparkOperator.template_fields + (
        'spark_class',
        'spark_jar',
        'ca_pod_template',
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
            os_credentials_secret_name: Optional[str] = 'opensearch-dags-credentials',
            os_cert_secret_name: Optional[str] = 'opensearch-ca-certificate',
            spark_config: Optional[str] = '',
            skip: Optional[bool] = False,
            **kwargs,
    ) -> None:
        self.spark_class = spark_class
        self.spark_jar = spark_jar
        self.spark_failure_msg = spark_failure_msg
        self.zone = zone
        self.spark_config = spark_config
        self.os_credentials_secret_name = os_credentials_secret_name
        self.os_cert_secret_name = os_cert_secret_name
        self.skip = skip
        super().__init__(
            spark_class=self.spark_class,
            spark_jar=self.spark_jar,
            spark_failure_msg=self.spark_failure_msg,
            zone=self.zone,
            spark_config=self.spark_config,
            **kwargs
        )


    def execute(self, **kwargs):
        if self.skip:
            raise AirflowSkipException()

        new_env_vars = [
            k8s.V1EnvVar(
                name='ES_USERNAME',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name=self.os_credentials_secret_name,
                        key='username')
                )
            ),
            k8s.V1EnvVar(
                name='ES_PASSWORD',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name=self.os_credentials_secret_name,
                        key='password')
                )
            ),
        ]

        if self.env_vars and self.es_credentials_secret_name:
            self.env_vars += new_env_vars
        else:
            self.env_vars = new_env_vars

        new_volumes = [
            k8s.V1Volume(
                name=self.es_cert_secret_name,
                secret=k8s.V1SecretVolumeSource(
                    secret_name='unic-prod-opensearch-ca-certificate',
                    default_mode=0o555
                ),
            )
        ]

        if self.volumes and self.es_cert_secret_name:
            self.volumes += new_volumes
        else:
            self.volumes = new_volumes

        new_volume_mounts = [
            k8s.V1VolumeMount(
                name=self.es_cert_secret_name,
                mount_path='/opt/os-ca',
                read_only=True,
            )
        ]

        if self.volume_mounts and self.es_cert_secret_name:
            self.volume_mounts += new_volume_mounts
        else:
            self.volume_mounts = new_volume_mounts

        super().execute(**kwargs)

