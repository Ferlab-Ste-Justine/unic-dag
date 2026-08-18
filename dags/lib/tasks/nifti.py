# pylint: disable=import-outside-toplevel, redefined-outer-name, reimported, too-many-locals
"""
Tasks converting the VNA clinique DICOMs to NIfTI.
"""
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, List, Mapping, Optional, Tuple

from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from kubernetes.client import models as k8s

from lib.config import DCM2NIIX_VERSION
from lib.nifti import list_study_folders, prefix_has_objects, read_accession_patterns, vna_s3_hook

# Dependencies installed into the convert_studies venv at task runtime.
CONVERT_REQUIREMENTS = [
    f"dcm2niix=={DCM2NIIX_VERSION}",
]

# Existence checks are one list_objects_v2 each, so they run concurrently. Kept under the connection
# pool vna_s3_hook sizes, since the hook's client is shared by every worker.
VERIFY_WORKERS = 16

# A cohort file can leave thousands of studies unmatched; the log names the first few.
MISSING_LOG_LIMIT = 20

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


def _validate_params(paths: Optional[List[str]], file_bucket: Optional[str],
                     file_key: Optional[str]) -> None:
    """
    Check that exactly one of the two study selection modes is fully specified.

    :param paths: Study prefixes given directly.
    :param file_bucket: Bucket holding the accession number file.
    :param file_key: Key of the accession number file.
    """
    if bool(file_bucket) != bool(file_key):
        raise AirflowFailException(
            "DAG params 'accession_file_bucket' and 'accession_file_key' must be provided together.")
    if paths and file_key:
        raise AirflowFailException(
            "DAG params 'paths' and 'accession_file_key' are mutually exclusive. Provide one or the other.")
    if not paths and not file_key:
        raise AirflowFailException(
            "One of DAG params 'paths' or 'accession_file_bucket' + 'accession_file_key' is required.")


def _resolve_patterns(s3: S3Hook, bucket: str, patterns: List[str]) -> Tuple[List[str], List[str]]:
    """
    Resolve patterns to the studies they match, deduped and in order of first appearance.

    :param s3: Hook for the bucket being walked.
    :param bucket: Bucket name to walk.
    :param patterns: Prefixes or wildcard patterns to resolve.
    :return: The study prefixes, and the patterns that matched none.
    """
    resolved, missing = [], []
    for pattern in patterns:
        found = list_study_folders(s3=s3, bucket=bucket, pattern=pattern)
        resolved += found
        if not found:
            missing.append(pattern)

    seen = set()  # dedupe, preserve order
    return [s for s in resolved if not (s in seen or seen.add(s))], missing


def _drop_empty_studies(s3: S3Hook, bucket: str, studies: List[str]) -> Tuple[List[str], List[str]]:
    """
    Split study prefixes into those holding objects and those holding none.

    `list_study_folders` returns an exact prefix without walking the bucket, so this is the only thing
    that rules out a study which does not exist yet.

    :param s3: Hook for the bucket being checked.
    :param bucket: Bucket name to check.
    :param studies: Study prefixes to check.
    :return: The prefixes holding objects, and those holding none.
    """
    # boto3 is only unsafe while a client is being created, and the hook caches its client without a
    # lock, so it is built here rather than by whichever workers race for it first. An exact study
    # prefix resolves without any listing, which leaves this as the first call to reach S3.
    s3.get_conn()

    with ThreadPoolExecutor(max_workers=VERIFY_WORKERS) as pool:
        has_objects = list(pool.map(
            lambda study: prefix_has_objects(s3=s3, bucket=bucket, prefix=study), studies))

    empty = {study for study, exists in zip(studies, has_objects) if not exists}
    return [study for study in studies if study not in empty], sorted(empty)


def _log_missing(missing: List[str], pattern_count: int, bucket: str) -> None:
    """
    Warn about everything that resolved to no study, naming the first few.

    :param missing: Patterns that matched nothing, and prefixes holding no object.
    :param pattern_count: How many patterns were resolved in total.
    :param bucket: Bucket that was searched.
    """
    if not missing:
        return

    shown = missing[:MISSING_LOG_LIMIT]
    logging.warning('%s of %s pattern(s) matched no study in %s, showing %s: %s',
                    len(missing), pattern_count, bucket, len(shown), shown)


@task(task_id="resolve_studies")
def resolve_studies(bucket: str, parent_prefix: str, verify_objects: bool = False,
                    params: Optional[Mapping[str, Any]] = None) -> List[str]:
    """
    Resolve the DAG params to the study prefixes to act on.

    :param bucket: Bucket holding the studies.
    :param parent_prefix: Prefix the studies live under, one per representation.
    :param verify_objects: If True, drop resolved prefixes holding no object. Needed whenever nothing
        downstream reports a study that does not exist.
    :param params: DAG params, injected by Airflow. Read instead of `dag_run.conf` because conf holds
        only what the trigger passed, so a partial conf from the API or the CLI would miss the params
        that have a default.
    """
    paths = params["paths"]
    file_bucket = params["accession_file_bucket"]
    file_key = params["accession_file_key"]

    _validate_params(paths=paths, file_bucket=file_bucket, file_key=file_key)

    if paths:
        patterns = list(paths)
    else:
        patterns = read_accession_patterns(
            s3=vna_s3_hook(file_bucket),
            bucket=file_bucket,
            key=file_key,
            accession_number_column=params["accession_number_column"],
            exam_date_column=params["exam_date_column"],
            parent_prefix=parent_prefix)

    s3 = vna_s3_hook(bucket)
    studies, missing = _resolve_patterns(s3=s3, bucket=bucket, patterns=patterns)

    if verify_objects:
        studies, empty = _drop_empty_studies(s3=s3, bucket=bucket, studies=studies)
        missing += empty

    _log_missing(missing=missing, pattern_count=len(patterns), bucket=bucket)

    if not studies:
        raise AirflowFailException(f"No study folder matched any of {len(patterns)} pattern(s)")

    logging.info('Resolved %s study folder(s) from %s pattern(s)', len(studies), len(patterns))
    return studies


@task(task_id="get_skip_existing")
def get_skip_existing(params=None) -> bool:
    return params['skip_existing']


@task.virtualenv(task_id="convert_studies", requirements=CONVERT_REQUIREMENTS,
                 system_site_packages=True, executor_config=CONVERT_EXECUTOR_CONFIG)
def convert_studies(studies: List[str], skip_existing: bool, run_stamp: str) -> None:
    """
    Convert every resolved study, a few at a time, then write the run's report.

    :param studies: Source study prefixes, as resolved by `resolve_studies`.
    :param skip_existing: True to leave studies whose NIfTI output already exists untouched.
    :param run_stamp: Identifier of the run, used to name the report.
    """
    import logging
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from typing import Tuple

    from airflow.exceptions import AirflowFailException

    from lib.config import VNA_CLINIQUE_RED_BUCKET, VNA_CLINIQUE_YELLOW_BUCKET
    from lib.nifti import PROBLEM_STATUSES, STUDY_WORKERS, ConversionStatus, \
        build_report_csv, conversion_result, convert_study, report_key, report_row, vna_s3_hook

    red_s3 = vna_s3_hook(VNA_CLINIQUE_RED_BUCKET)
    yellow_s3 = vna_s3_hook(VNA_CLINIQUE_YELLOW_BUCKET)

    # boto3 is only unsafe while a client is being created, so build both before the workers start.
    # The clients themselves are thread-safe, which is what lets one hook per bucket be shared below.
    red_s3.get_conn()
    yellow_s3.get_conn()

    def _convert(study: str) -> Tuple[str, dict]:
        try:
            return study, convert_study(study=study, red_s3=red_s3, yellow_s3=yellow_s3,
                                        skip_existing=skip_existing)
        # Broad on purpose: a failure has to stay scoped to its own study, because one raising study
        # would otherwise abandon the rest of the batch. Enumerating what dcm2niix, zlib, botocore and
        # the filesystem can raise would fail the batch on whatever the tuple missed.
        except Exception as e:  # pylint: disable=broad-exception-caught
            logging.exception('%s: conversion failed', study)  # keep the traceback for unexpected errors
            return study, conversion_result(ConversionStatus.FAILED, output=f"{type(e).__name__}: {e}",
                                            detail=f"{type(e).__name__}: {e}")

    # Results are consumed as they land rather than all at the end, so a long backfill reports progress
    # instead of staying silent until the last study.
    results = []
    with ThreadPoolExecutor(max_workers=STUDY_WORKERS) as executor:
        futures = [executor.submit(_convert, study) for study in studies]
        for future in as_completed(futures):
            study, result = future.result()
            results.append((study, result))
            log = logging.warning if result["status"] in PROBLEM_STATUSES else logging.info
            log('[%s/%s] %s: %s (%s)', len(results), len(studies), study,
                result["status"], result["detail"])

    # Every status is reported in a fixed order, zeros included, so the line reads the same from one
    # run to the next.
    statuses = [result["status"] for _, result in results]
    logging.info('out of %s study folder(s): %s', len(statuses),
                 ', '.join(f'{statuses.count(status)} {status}' for status in ConversionStatus))

    report_rows = [report_row(study, result) for study, result in results
                   if result["status"] in PROBLEM_STATUSES]
    key = report_key(run_stamp)
    yellow_s3.load_string(string_data=build_report_csv(report_rows), key=key,
                          bucket_name=VNA_CLINIQUE_YELLOW_BUCKET, replace=True)
    logging.info('reported %s problematic study folder(s) in %s/%s',
                 len(report_rows), VNA_CLINIQUE_YELLOW_BUCKET, key)

    # Only a hard failure fails the task. A partial conversion was uploaded, and a report-only or
    # absent study is a source-data fact that no retry can change; both are in the report instead.
    failed = [study for study, result in results if result["status"] == ConversionStatus.FAILED]
    if failed:
        raise AirflowFailException(f"{len(failed)} of {len(results)} studies failed: {failed[:20]}")
