"""
Tasks moving the preterm imaging data between UnIC and SD4Health.
"""
from typing import List

from airflow.decorators import task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.utils.pod_manager import OnFinishAction

from lib.config import FREESURFER_STATS_PREFIX, VNA_CLINIQUE_YELLOW_BUCKET
from lib.sd4h import CEPH_BUCKET, CEPH_REMOTE, FREESURFER_STATS_PATH, MINIO_REMOTE, POD_NAMESPACE, \
    POD_SERVICE_ACCOUNT, RCLONE_IMAGE, RCLONE_TRANSFERS, VM_REMOTE, ceph_remote_env, \
    minio_remote_env, pod_resources, ssh_key_volume, ssh_key_volume_mount, vm_remote_env

# One `rclone copy` per study prefix rather than one filtered copy of the whole tree: rclone tests
# every filter rule against every path it lists, so a cohort of thousands of studies would spend its
# time matching rules instead of transferring. Each prefix arrives as its own argument, read through
# "$@", because the kernel caps a single argument at 128 KB.
TRANSFER_SCRIPT = f"""
set -u
total=$#
count=0
for study in "$@"; do
  count=$((count + 1))
  echo "[$count/$total] $study"
  rclone copy "{MINIO_REMOTE}:{VNA_CLINIQUE_YELLOW_BUCKET}/$study" \\
              "{CEPH_REMOTE}:{CEPH_BUCKET}/$study" \\
              --transfers={RCLONE_TRANSFERS} --stats=0
  rc=$?
  # 3 is a source prefix that vanished after it was resolved. Any other non-zero code is a real
  # failure, and stopping on it keeps a broken remote from being retried once per remaining study.
  if [ "$rc" -ne 0 ] && [ "$rc" -ne 3 ]; then
    echo "rclone exited $rc on $study"
    exit "$rc"
  fi
done
echo "transferred $count study prefix(es)"
"""


@task(task_id="build_transfer_args")
def build_transfer_args(studies: List[str]) -> List[str]:
    """
    Build the transfer pod's argv from the resolved study prefixes.

    `--` stands in for `$0`, so the prefixes land in `"$@"`.

    :param studies: Study prefixes to transfer, relative to the source bucket root.
    """
    return [TRANSFER_SCRIPT, "--", *studies]


def transfer_studies(arguments: List[str]) -> KubernetesPodOperator:
    """
    Copy the given study prefixes from the yellow MinIO to the SD4Health object store.

    :param arguments: Pod argv, as built by `build_transfer_args`.
    """
    return KubernetesPodOperator(
        task_id="transfer_studies",
        name="sd4h-preterm-nifti-transfer",
        namespace=POD_NAMESPACE,
        service_account_name=POD_SERVICE_ACCOUNT,
        image=RCLONE_IMAGE,
        cmds=["/bin/sh", "-c"],
        arguments=arguments,
        env_vars=minio_remote_env() + ceph_remote_env(),
        container_resources=pod_resources(),
        # Keep a failed pod for inspection, reap it once it has succeeded.
        on_finish_action=OnFinishAction.DELETE_SUCCEEDED_POD,
        get_logs=True)


def pull_stats() -> KubernetesPodOperator:
    """
    Copy the FreeSurfer `.stats` files from the SD4Health VM to the yellow MinIO.
    """
    return KubernetesPodOperator(
        task_id="pull_stats",
        name="sd4h-preterm-stats-pull",
        namespace=POD_NAMESPACE,
        service_account_name=POD_SERVICE_ACCOUNT,
        image=RCLONE_IMAGE,
        cmds=["rclone"],
        arguments=[
            "copy",
            f"{VM_REMOTE}:{FREESURFER_STATS_PATH}",
            f"{MINIO_REMOTE}:{VNA_CLINIQUE_YELLOW_BUCKET}/{FREESURFER_STATS_PREFIX}/",
            "--include", "*.stats",
            f"--transfers={RCLONE_TRANSFERS}",
            "--stats=0",
        ],
        env_vars=minio_remote_env() + vm_remote_env(),
        volumes=[ssh_key_volume()],
        volume_mounts=[ssh_key_volume_mount()],
        container_resources=pod_resources(),
        on_finish_action=OnFinishAction.DELETE_SUCCEEDED_POD,
        get_logs=True)
