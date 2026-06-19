"""
DAG to transfer anonymized brain MRI NIfTIs from UnIC yellow MinIO to SD4Health VM.
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
vers la VM SD4Health (`chusj-brain-mri` @ 198.168.188.36, `/data/UnIC/`) pour
traitement FreeSurfer.

Credentials MinIO : user `brain-mri-sd4health`, secret dans le K8s secret
`brain-mri-yellow-minio`. Clé SSH SFTP dans `brain-mri-ssh-key`.

Trigger manuel uniquement depuis l'UI Airflow.
"""

YELLOW_MINIO_ENDPOINT = "https://minio.unic.ferlab.bio"
YELLOW_MINIO_USER = "brain-mri-sd4health"
YELLOW_BUCKET = "vna-clinique-yellow"
NIFTI_PATH = "nifti"
SD4H_VM_HOST = "198.168.188.36"
SD4H_VM_USER = "ubuntu"
SD4H_DEST_PATH = "/data/UnIC"

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
            f'sd4h:{SD4H_DEST_PATH}/',
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
            k8s.V1EnvVar(name='RCLONE_CONFIG_SD4H_TYPE', value='sftp'),
            k8s.V1EnvVar(name='RCLONE_CONFIG_SD4H_HOST', value=SD4H_VM_HOST),
            k8s.V1EnvVar(name='RCLONE_CONFIG_SD4H_USER', value=SD4H_VM_USER),
            k8s.V1EnvVar(name='RCLONE_CONFIG_SD4H_KEY_FILE', value='/etc/rclone-ssh/id_ed25519'),
            k8s.V1EnvVar(name='RCLONE_CONFIG_SD4H_KNOWN_HOSTS_FILE', value='/etc/rclone-ssh/known_hosts'),
        ],
        volumes=[
            k8s.V1Volume(
                name='rclone-ssh-key',
                secret=k8s.V1SecretVolumeSource(
                    secret_name='brain-mri-ssh-key',
                    default_mode=0o400,
                ),
            ),
        ],
        volume_mounts=[
            k8s.V1VolumeMount(
                name='rclone-ssh-key',
                mount_path='/etc/rclone-ssh',
                read_only=True,
            ),
        ],
        is_delete_operator_pod=False,
        get_logs=True,
    )

    start() >> transfer >> end()
