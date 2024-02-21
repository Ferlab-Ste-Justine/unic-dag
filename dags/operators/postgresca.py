from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

from kubernetes.client import models as k8s

class PostgresCaOperator(PostgresOperator, KubernetesPodOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(is_delete_operator_pod=False, **kwargs)

    def execute(self, **kwargs):
        self.volumes = [
            k8s.V1Volume(
                name='ca-certificate',
                config_map=k8s.V1ConfigMapVolumeSource(
                    name='unic-prod-bi-postgres-ca-cert',
                ),
            ),
        ]
        self.volume_mounts = [
            k8s.V1VolumeMount(
                name='ca-certificate',
                mount_path='/opt/ca/bi',
                read_only=True,
            ),
        ]

        KubernetesPodOperator.execute(**kwargs)
        PostgresOperator.execute(**kwargs)
