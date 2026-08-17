"""
rclone plumbing for the SD4Health transfers of the preterm imaging data.

Both directions run rclone in a pod and configure their remotes entirely through
`RCLONE_CONFIG_<REMOTE>_<OPTION>` environment variables, so no config file is ever written to disk.
Three remotes are involved: the UnIC yellow MinIO (`yellow`), the SD4Health object store on Juno
(`ceph`), and the SD4Health VM over SFTP (`sd4h`).
"""
from typing import List

from kubernetes.client import models as k8s

# The SD4Health MinIO user, its Ceph bucket and the three Kubernetes secrets below are named after
# brain-mri, which is what the preterm project was called when they were provisioned. They are
# created in unic-kubernetes-environments and on the SD4Health side, so the names stay as they are.
MINIO_USER = "brain-mri-sd4health"
CEPH_BUCKET = "chusj-brain-mri"
MINIO_SECRET = "brain-mri-yellow-minio"
CEPH_SECRET = "brain-mri-ceph-credentials"
SSH_KEY_SECRET = "brain-mri-ssh-key"

MINIO_ENDPOINT = "https://minio.unic.ferlab.bio"
CEPH_ENDPOINT = "https://objets.juno.calculquebec.ca"

# SD4Health VM hosting the FreeSurfer outputs, reachable at a public address.
VM_HOST = "198.168.188.36"
VM_USER = "ubuntu"

# Absolute path of the FreeSurfer stats tree on the VM. The stats pull stays paused until it is set.
# TODO: confirm the FreeSurfer layout with the SD4Health team  # pylint: disable=fixme
FREESURFER_STATS_PATH = "/data/UnIC/PLACEHOLDER_FREESURFER_STATS_PATH/"

# rclone remote names. Paths reference them in lowercase, the env vars in uppercase.
MINIO_REMOTE = "yellow"
CEPH_REMOTE = "ceph"
VM_REMOTE = "sd4h"

SSH_KEY_MOUNT_PATH = "/etc/rclone-ssh"
SSH_KEY_VOLUME = "rclone-ssh-key"

RCLONE_IMAGE = "rclone/rclone:1.75.0"

# Concurrent file transfers within one rclone call.
RCLONE_TRANSFERS = 8

# Airflow's own namespace. A zone namespace is for datalake ETL scoped to that zone, whereas these
# pods are an outside process moving data in and out of the datalake. unic-prod provisions the same
# `spark` service account, so they run under it rather than under `default`.
POD_NAMESPACE = "unic-prod"
POD_SERVICE_ACCOUNT = "spark"

# rclone streams objects through memory rather than staging them on disk, so this is sized for the
# per-transfer buffers rather than for the data volume.
POD_MEMORY = "2Gi"
POD_CPU = "2"


def _env(name: str, value: str) -> k8s.V1EnvVar:
    return k8s.V1EnvVar(name=name, value=value)


def _secret_env(name: str, secret_name: str, key: str) -> k8s.V1EnvVar:
    return k8s.V1EnvVar(
        name=name,
        value_from=k8s.V1EnvVarSource(
            secret_key_ref=k8s.V1SecretKeySelector(name=secret_name, key=key)))


def minio_remote_env() -> List[k8s.V1EnvVar]:
    """rclone remote for the UnIC yellow MinIO, keyed by the dedicated SD4Health transfer user."""
    remote = MINIO_REMOTE.upper()
    return [
        _env(f"RCLONE_CONFIG_{remote}_TYPE", "s3"),
        _env(f"RCLONE_CONFIG_{remote}_PROVIDER", "Minio"),
        _env(f"RCLONE_CONFIG_{remote}_ENDPOINT", MINIO_ENDPOINT),
        _env(f"RCLONE_CONFIG_{remote}_ENV_AUTH", "false"),
        _env(f"RCLONE_CONFIG_{remote}_ACCESS_KEY_ID", MINIO_USER),
        _secret_env(f"RCLONE_CONFIG_{remote}_SECRET_ACCESS_KEY", MINIO_SECRET, "secret-key"),
    ]


def ceph_remote_env() -> List[k8s.V1EnvVar]:
    """
    rclone remote for the SD4Health object store on Juno.

    `NO_CHECK_BUCKET` because the transfer user can write to its bucket but not query its existence,
    which rclone otherwise does before the first upload.
    """
    remote = CEPH_REMOTE.upper()
    return [
        _env(f"RCLONE_CONFIG_{remote}_TYPE", "s3"),
        _env(f"RCLONE_CONFIG_{remote}_PROVIDER", "Other"),
        _env(f"RCLONE_CONFIG_{remote}_ENDPOINT", CEPH_ENDPOINT),
        _env(f"RCLONE_CONFIG_{remote}_ENV_AUTH", "false"),
        _env(f"RCLONE_CONFIG_{remote}_NO_CHECK_BUCKET", "true"),
        _secret_env(f"RCLONE_CONFIG_{remote}_ACCESS_KEY_ID", CEPH_SECRET, "access-key"),
        _secret_env(f"RCLONE_CONFIG_{remote}_SECRET_ACCESS_KEY", CEPH_SECRET, "secret-key"),
    ]


def vm_remote_env() -> List[k8s.V1EnvVar]:
    """rclone remote for the SD4Health VM over SFTP, authenticated by the mounted SSH key."""
    remote = VM_REMOTE.upper()
    return [
        _env(f"RCLONE_CONFIG_{remote}_TYPE", "sftp"),
        _env(f"RCLONE_CONFIG_{remote}_HOST", VM_HOST),
        _env(f"RCLONE_CONFIG_{remote}_USER", VM_USER),
        _env(f"RCLONE_CONFIG_{remote}_KEY_FILE", f"{SSH_KEY_MOUNT_PATH}/id_ed25519"),
        _env(f"RCLONE_CONFIG_{remote}_KNOWN_HOSTS_FILE", f"{SSH_KEY_MOUNT_PATH}/known_hosts"),
    ]


def ssh_key_volume() -> k8s.V1Volume:
    """The SSH key the SFTP remote authenticates with. Mode 0400 or OpenSSH refuses the key."""
    return k8s.V1Volume(
        name=SSH_KEY_VOLUME,
        secret=k8s.V1SecretVolumeSource(secret_name=SSH_KEY_SECRET, default_mode=0o400))


def ssh_key_volume_mount() -> k8s.V1VolumeMount:
    return k8s.V1VolumeMount(name=SSH_KEY_VOLUME, mount_path=SSH_KEY_MOUNT_PATH, read_only=True)


def pod_resources() -> k8s.V1ResourceRequirements:
    return k8s.V1ResourceRequirements(
        requests={"memory": POD_MEMORY, "cpu": POD_CPU},
        limits={"memory": POD_MEMORY, "cpu": POD_CPU})
