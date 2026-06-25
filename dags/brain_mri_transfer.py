"""
DAG to transfer anonymized brain MRI NIfTIs from UnIC yellow MinIO to SD4Health Ceph bucket.
"""
from datetime import datetime

import pendulum
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s

from lib.config import DEFAULT_ARGS
from lib.slack import Slack
from lib.tasks.notify import start, end

DOC = """
# Brain MRI Transfer DAG

Transfère les NIfTIs anonymisés depuis `vna-clinique-yellow/nifti/`
vers le bucket Swift SD4Health `chusj-brain-mri` sur Calcul Québec (Juno).

Credentials MinIO : user `brain-mri-sd4health`, secret dans le K8s secret
`brain-mri-yellow-minio`. Credentials Ceph EC2 dans `brain-mri-ceph-credentials`.

Trigger manuel uniquement depuis l'UI Airflow.
"""

YELLOW_MINIO_ENDPOINT = "https://minio.unic.ferlab.bio"
YELLOW_MINIO_USER = "brain-mri-sd4health"
YELLOW_BUCKET = "vna-clinique-yellow"
NIFTI_PATH = "nifti"
CEPH_ENDPOINT = "https://objets.juno.calculquebec.ca"
CEPH_BUCKET = "chusj-brain-mri"

LOCAL_TZ = pendulum.timezone("America/Montreal")

dag = DAG(
    dag_id="brain_mri_transfer",
    doc_md=DOC,
    start_date=datetime(2026, 6, 1, tzinfo=LOCAL_TZ),
    schedule_interval=None,
    default_args=DEFAULT_ARGS,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=1,
    tags=['brain-mri', 'sd4health'],
    on_failure_callback=Slack.notify_dag_failure,
)

with dag:
    transfer = KubernetesPodOperator(
        task_id='rclone_nifti_transfer',
        name='brain-mri-rclone-transfer',
        namespace='unic-prod',
        image='rclone/rclone:1.65',
        cmds=['rclone'],
        arguments=[
            'copy',
            f'yellow:{YELLOW_BUCKET}/{NIFTI_PATH}/',
            f'ceph:{CEPH_BUCKET}/nifti/',
            '--progress',
            '--log-level=INFO',
            '--transfers=4',
        ],
        env_vars=[
            k8s.V1EnvVar(name='RCLONE_CONFIG_YELLOW_TYPE', value='s3'),
            k8s.V1EnvVar(name='RCLONE_CONFIG_YELLOW_PROVIDER', value='Minio'),
            k8s.V1EnvVar(name='RCLONE_CONFIG_YELLOW_ACCESS_KEY_ID', value=YELLOW_MINIO_USER),
            k8s.V1EnvVar(
                name='RCLONE_CONFIG_YELLOW_SECRET_ACCESS_KEY',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name='brain-mri-yellow-minio',
                        key='secret-key',
                    )
                )
            ),
            k8s.V1EnvVar(name='RCLONE_CONFIG_YELLOW_ENDPOINT', value=YELLOW_MINIO_ENDPOINT),
            k8s.V1EnvVar(name='RCLONE_CONFIG_YELLOW_ENV_AUTH', value='false'),
            k8s.V1EnvVar(name='RCLONE_CONFIG_CEPH_TYPE', value='s3'),
            k8s.V1EnvVar(name='RCLONE_CONFIG_CEPH_PROVIDER', value='Other'),
            k8s.V1EnvVar(
                name='RCLONE_CONFIG_CEPH_ACCESS_KEY_ID',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name='brain-mri-ceph-credentials',
                        key='access-key',
                    )
                )
            ),
            k8s.V1EnvVar(
                name='RCLONE_CONFIG_CEPH_SECRET_ACCESS_KEY',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name='brain-mri-ceph-credentials',
                        key='secret-key',
                    )
                )
            ),
            k8s.V1EnvVar(name='RCLONE_CONFIG_CEPH_ENDPOINT', value=CEPH_ENDPOINT),
            k8s.V1EnvVar(name='RCLONE_CONFIG_CEPH_ENV_AUTH', value='false'),
            k8s.V1EnvVar(name='RCLONE_CONFIG_CEPH_NO_CHECK_BUCKET', value='true'),
        ],
        is_delete_operator_pod=False,
        get_logs=True,
    )

    start() >> transfer >> end()
