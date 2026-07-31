# pylint: disable=import-outside-toplevel, redefined-outer-name, reimported, too-many-locals
"""
Tasks converting the VNA clinique DICOMs to NIfTI.
"""
import logging
from typing import List

from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from kubernetes.client import models as k8s

from lib.config import DCM2NIIX_VERSION, VNA_CLINIQUE_RED_BUCKET
from lib.nifti import list_study_folders, read_accession_patterns, vna_s3_hook

# Dependencies installed into the convert_studies venv at task runtime.
CONVERT_REQUIREMENTS = [
    f"dcm2niix=={DCM2NIIX_VERSION}",
]

# convert_studies runs dcm2niix in its own KubernetesExecutor pod. The Helm values leave
# `workers.resources` unset, so without this the pod is unbounded on a node that reserves nothing.
# executor_config / pod_override is resolved at DAG-parse time and is NOT Jinja-templatable
# CPU matches STUDY_WORKERS, since dcm2niix is single threaded per call. Staged DICOMs live in the pod's
# own filesystem, hence the ephemeral-storage reservation.
CONVERT_POD_MEMORY = "8Gi"
CONVERT_POD_CPU = "8"
CONVERT_POD_EPHEMERAL_STORAGE = "32Gi"
CONVERT_EXECUTOR_CONFIG = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    # The worker container is named "base" so the override targets it.
                    name="base",
                    resources=k8s.V1ResourceRequirements(
                        requests={"memory": CONVERT_POD_MEMORY, "cpu": CONVERT_POD_CPU,
                                  "ephemeral-storage": CONVERT_POD_EPHEMERAL_STORAGE},
                        limits={"memory": CONVERT_POD_MEMORY, "cpu": CONVERT_POD_CPU,
                                "ephemeral-storage": CONVERT_POD_EPHEMERAL_STORAGE},
                    ),
                )
            ]
        )
    )
}


@task(task_id="resolve_studies")
def resolve_studies(params=None) -> List[str]:
    """
    Resolve the DAG params to the source study prefixes to convert.

    Reads `params` rather than `dag_run.conf`: conf holds only what the trigger passed, so a partial
    conf from the API or the CLI would miss the params that have a default.
    """
    paths = params["paths"]
    file_bucket = params["accession_file_bucket"]
    file_key = params["accession_file_key"]
    accession_number_column = params["accession_number_column"]
    exam_date_column = params["exam_date_column"]

    if bool(file_bucket) != bool(file_key):
        raise AirflowFailException(
            "DAG params 'accession_file_bucket' and 'accession_file_key' must be provided together.")
    if paths and file_key:
        raise AirflowFailException(
            "DAG params 'paths' and 'accession_file_key' are mutually exclusive. Provide one or the other.")
    if not paths and not file_key:
        raise AirflowFailException(
            "One of DAG params 'paths' or 'accession_file_bucket' + 'accession_file_key' is required.")

    if paths:
        patterns = list(paths)
    else:
        patterns = read_accession_patterns(
            s3=vna_s3_hook(file_bucket),
            bucket=file_bucket,
            key=file_key,
            accession_number_column=accession_number_column,
            exam_date_column=exam_date_column)

    red_s3 = vna_s3_hook(VNA_CLINIQUE_RED_BUCKET)
    resolved = []
    for pattern in patterns:
        resolved += list_study_folders(s3=red_s3, bucket=VNA_CLINIQUE_RED_BUCKET, pattern=pattern)

    seen = set()  # dedupe, preserve order
    studies = [s for s in resolved if not (s in seen or seen.add(s))]

    if not studies:
        raise AirflowFailException(f"No study folder matched any of {len(patterns)} pattern(s)")

    logging.info('Resolved %s study folder(s) from %s pattern(s)', len(studies), len(patterns))
    return studies


@task(task_id="get_skip_existing")
def get_skip_existing(params=None) -> bool:
    return params['skip_existing']


@task.virtualenv(task_id="convert_studies", requirements=CONVERT_REQUIREMENTS,
                 system_site_packages=True, executor_config=CONVERT_EXECUTOR_CONFIG)
def convert_studies(studies: List[str], skip_existing: bool) -> None:
    """
    Convert every resolved study, a few at a time.

    :param studies: Source study prefixes, as resolved by `resolve_studies`.
    :param skip_existing: True to leave studies whose NIfTI output already exists untouched.
    """
    import logging
    from concurrent.futures import ThreadPoolExecutor
    from typing import Tuple

    from airflow.exceptions import AirflowFailException

    from lib.config import VNA_CLINIQUE_RED_BUCKET, VNA_CLINIQUE_YELLOW_BUCKET
    from lib.nifti import STUDY_WORKERS, convert_study, vna_s3_hook

    red_s3 = vna_s3_hook(VNA_CLINIQUE_RED_BUCKET)
    yellow_s3 = vna_s3_hook(VNA_CLINIQUE_YELLOW_BUCKET)

    # boto3 is only unsafe while a client is being created, so build both before the workers start.
    # The clients themselves are thread-safe, which is what lets one hook per bucket be shared below.
    red_s3.get_conn()
    yellow_s3.get_conn()

    def _convert(study: str) -> Tuple[str, str]:
        try:
            return study, convert_study(study=study, red_s3=red_s3, yellow_s3=yellow_s3,
                                        skip_existing=skip_existing)
        # Broad on purpose: a failure has to stay scoped to its own study, because executor.map drops
        # every result once one item raises, successes included. Enumerating what dcm2niix, zlib,
        # botocore and the filesystem can raise would fail the batch on whatever the tuple missed.
        except Exception as e:  # pylint: disable=broad-exception-caught
            logging.exception('%s: conversion failed', study)  # keep the traceback for unexpected errors
            return study, f"FAILED: {type(e).__name__}: {e}"

    with ThreadPoolExecutor(max_workers=STUDY_WORKERS) as executor:
        results = list(executor.map(_convert, studies))

    for study, status in results:
        if status.startswith(("WARNING", "FAILED")):
            logging.warning('%s: %s', study, status)
        else:
            logging.info('%s: %s', study, status)

    failed = [study for study, status in results if status.startswith("FAILED")]
    logging.info('%s converted, %s failed, out of %s study folder(s)',
                 len(results) - len(failed), len(failed), len(results))
    if failed:
        raise AirflowFailException(f"{len(failed)} of {len(results)} studies failed: {failed[:20]}")
