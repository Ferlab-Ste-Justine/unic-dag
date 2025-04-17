from typing import Sequence, Callable

from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from kubernetes.client import models as k8s

class PythonOpenSearchOperator(PythonOperator):
    """
    Execute Python with OpenSearch Connection

    :param: python_callable: The connection ID used to connect to Postgres
    :param: os_cert_secret_name
    :param: os_credentials_secret_name
    :param: os_credentials_secret_key_username
    :param: os_credentials_secret_key_password
    """
    template_fields: Sequence[str] =  (*PythonOperator.template_fields, 'skip', 'python_callable')

    def __init__(
            self,
            python_callable: Callable,
            os_cert_secret_name: str,
            os_credentials_secret_name: str,
            os_credentials_secret_key_username: str,
            os_credentials_secret_key_password: str,
            skip: bool = False,
            **kwargs) -> None:
        # python_callable has to be rendered before super class is instantiated
        self.python_callable = python_callable
        self.os_cert_secret_name = os_cert_secret_name
        self.os_credentials_secret_name = os_credentials_secret_name
        self.os_credentials_secret_key_username = os_credentials_secret_key_username
        self.os_credentials_secret_key_password = os_credentials_secret_key_password
        self.skip = skip
        super().__init__(python_callable=self.python_callable, **kwargs)

    def execute(self, **kwargs):
        if self.skip:
            raise AirflowSkipException()

        new_env_vars = [
            k8s.V1EnvVar(
                name='OS_USERNAME',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name=self.os_credentials_secret_name,
                        key=self.os_credentials_secret_key_username)
                )
            ),
            k8s.V1EnvVar(
                name='OS_PASSWORD',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name=self.os_credentials_secret_name,
                        key=self.os_credentials_secret_key_password)
                )
            )
        ]

        if self.env_vars and self.os_credentials_secret_name:
            self.env_vars += new_env_vars
        else:
            self.env_vars = new_env_vars

        new_volumes = [
            k8s.V1Volume(
                name=self.os_cert_secret_name,
                secret=k8s.V1SecretVolumeSource(
                    secret_name=self.os_cert_secret_name,
                    default_mode=0o555
                ),
            )
        ]

        if self.volumes and self.os_cert_secret_name:
            self.volumes += new_volumes
        else:
            self.volumes = new_volumes

        new_volume_mounts = [
            k8s.V1VolumeMount(
                name=self.os_cert_secret_name,
                mount_path='/opt/os-ca',
                read_only=True,
            )
        ]

        if self.volume_mounts and self.os_cert_secret_name:
            self.volume_mounts += new_volume_mounts
        else:
            self.volume_mounts = new_volume_mounts

        super().execute(**kwargs)
