# pylint: disable=too-many-locals
"""
DICOM to NIfTI conversion for the VNA clinique imaging buckets.

Source DICOMs are Orthanc-compressed (`.cmp`) and laid out as
`dicoms/<year>/<month>/<day>/<accession number>/`. Outputs keep that date structure under one prefix
per representation: `nifti/` in the yellow bucket and `nifti_sidecars/` in the red one.
"""
import fnmatch
import io
import os
import re
import shutil
import subprocess
import sys
import tempfile
import zlib
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from typing import List

import pandas as pd
from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.config import Config

from lib.config import DICOM_PREFIX, MINIO_CONN_ID, NIFTI_PREFIX, NIFTI_SIDECARS_PREFIX, \
    VNA_CLINIQUE_RED_BUCKET, VNA_CLINIQUE_YELLOW_BUCKET
from lib.publish_utils import determine_minio_conn_id_from_config

# Orthanc StorageCompression (.cmp) layout
ORTHANC_HEADER_LEN = 8  # bytes Orthanc prepends before the zlib stream

# DICOM Part 10 layout, used to validate the decompressed result
DICOM_PREAMBLE_LEN = 128  # bytes of preamble before the magic marker
DICOM_MAGIC = b"DICM"  # 4-byte marker immediately after the preamble
DICOM_MAGIC_END = DICOM_PREAMBLE_LEN + len(DICOM_MAGIC)  # 132: end of marker slice

# Study folders are named <two letters><digits…> (RA2026…, XY2026…); date folders are all digits.
# This tells the two apart while walking the tree.
STUDY_RE = re.compile(r"^[A-Za-z]{2}\d")

# dcm2niix is single threaded per call, so this is also the number of cores the conversion can use. It
# is sized against the CPU the task pod requests, see CONVERT_STUDIES_EXECUTOR_CONFIG.
STUDY_WORKERS = 8  # studies converted concurrently
DOWNLOAD_WORKERS = 8  # objects downloaded concurrently within a study, so 64 concurrent GET at most

# Generous ceiling on a single dcm2niix call. The DAG run itself is unbounded, so without this a study
# dcm2niix cannot parse would stall its worker for the lifetime of the run.
DCM2NIIX_TIMEOUT_SECONDS = 1800


def orthanc_decompress(raw: bytes) -> bytes:
    """
    Decompress an Orthanc StorageCompression (.cmp) blob back to DICOM bytes.

    :param raw: Raw `.cmp` object contents.
    """
    # Skip Orthanc's fixed header, then inflate the zlib stream.
    out = zlib.decompress(raw[ORTHANC_HEADER_LEN:])
    if len(out) >= DICOM_MAGIC_END and out[DICOM_PREAMBLE_LEN:DICOM_MAGIC_END] == DICOM_MAGIC:
        return out
    raise ValueError("not a valid DICOM blob (no DICM after decompress)")


def parse_exam_date(value: str) -> date:
    """
    Parse an exam date from the accession-number file. `YYYY-MM-DD` only.

    Ambiguous formats such as `03/04/2026` are rejected rather than guessed: reading the day as the
    month would build a prefix that silently matches no study. The format is pinned rather than
    delegated to `date.fromisoformat`, which widened what it accepts in Python 3.11.

    :param value: Date as written in the file.
    """
    try:
        return datetime.strptime(str(value).strip(), "%Y-%m-%d").date()
    except ValueError as e:
        raise AirflowFailException(f"exam date '{value}' is not ISO 8601 (e.g. 2026-01-15)") from e


def study_pattern(exam_date: date, accession_number: str) -> str:
    """
    Build a source study prefix from an exam date and an accession number.

    :param exam_date: Date of the exam.
    :param accession_number: Accession number, may contain `*` wildcards.
    """
    return f"{DICOM_PREFIX}/{exam_date:%Y/%m/%d}/{accession_number}"


def get_output_prefix(study: str, parent_prefix: str) -> str:
    """
    Where a study's output goes, preserving the date structure of the source.

    `dicoms/2026/01/02/RA202600030101` becomes `nifti/2026/01/02/RA202600030101`.

    :param study: Source study prefix relative to the bucket root.
    :param parent_prefix: Output parent prefix, one per representation.
    """
    parts = study.rstrip("/").split("/", 1)
    if len(parts) != 2:
        raise ValueError(f"study prefix is missing its parent prefix: {study}")
    return f"{parent_prefix}/{parts[1]}"


def vna_s3_hook(bucket: str) -> S3Hook:
    """
    Hook for a VNA bucket, routed to its zone's Minio connection.

    The connection pool is sized to the concurrency the conversion drives, since botocore defaults to
    10 and one client is shared by every worker. Too small a pool still works, but urllib3 discards
    the surplus connections instead of reusing them, so each request pays a fresh TLS handshake.

    :param bucket: VNA bucket name.
    """
    return S3Hook(aws_conn_id=determine_minio_conn_id_from_config(MINIO_CONN_ID, bucket),
                  config=Config(max_pool_connections=STUDY_WORKERS * DOWNLOAD_WORKERS))


def read_accession_patterns(s3: S3Hook, bucket: str, key: str,
                            accession_number_column: str, exam_date_column: str) -> List[str]:
    """
    Read a CSV of accession numbers and exam dates into a list of source study prefixes.

    :param s3: Hook for the bucket holding the file.
    :param bucket: Bucket name of the CSV file.
    :param key: Key of the CSV file.
    :param accession_number_column: Column holding the accession numbers, wildcards allowed in values.
    :param exam_date_column: Column holding the exam dates, `YYYY-MM-DD` only.
    """
    csv_data = s3.get_key(key=key, bucket_name=bucket).get()['Body'].read()
    # Everything as string: accession numbers can carry leading zeros, and dates must not be guessed
    # at by pandas.
    df = pd.read_csv(io.BytesIO(csv_data), dtype=str)

    missing = [c for c in (accession_number_column, exam_date_column) if c not in df.columns]
    if missing:
        raise AirflowFailException(
            f"column(s) {missing} not found in {bucket}/{key}. Found: {list(df.columns)}")

    df = df[[accession_number_column, exam_date_column]].dropna()
    return [study_pattern(exam_date=parse_exam_date(row[exam_date_column]),
                          accession_number=str(row[accession_number_column]).strip())
            for _, row in df.iterrows()]


def list_study_folders(s3: S3Hook, bucket: str, pattern: str) -> List[str]:
    """
    Resolve a pattern to the leaf study folders it matches.

    `pattern` is a prefix at any depth (`dicoms/2026`, `dicoms/2026/01/02`,
    `dicoms/2026/01/02/RA202600030101`) and may contain `*` wildcards in any segment
    (`dicoms/2026/01/02/RA2026000301*`, `dicoms/2026/*/*/RA*`). Folders are walked rather than listed
    recursively so that a broad pattern does not page through every object in the bucket.

    :param s3: Hook for the bucket being walked.
    :param bucket: Bucket name to walk.
    :param pattern: Prefix or wildcard pattern relative to the bucket root.
    """
    pattern = pattern.rstrip("/")
    has_glob = "*" in pattern

    # An exact study folder needs no walk.
    if not has_glob and STUDY_RE.match(pattern.split("/")[-1]):
        return [pattern]

    # The literal prefix is every segment before the first one holding a wildcard.
    literal = []
    for segment in pattern.split("/"):
        if "*" in segment:
            break
        literal.append(segment)
    base = ("/".join(literal) + "/") if literal else ""

    paginator = s3.get_conn().get_paginator("list_objects_v2")
    found, stack = [], [base]
    while stack:
        prefix = stack.pop()
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
            for common_prefix in page.get("CommonPrefixes", []):
                folder = common_prefix["Prefix"].rstrip("/")
                if STUDY_RE.match(folder.split("/")[-1]):  # study folder, keep
                    found.append(folder)
                else:  # date folder, recurse
                    stack.append(common_prefix["Prefix"])

    if has_glob:
        found = [f for f in found if fnmatch.fnmatchcase(f, pattern)]
    return sorted(found)


def prefix_has_objects(s3: S3Hook, bucket: str, prefix: str) -> bool:
    """
    Whether any object exists under a folder prefix.

    :param s3: Hook for the bucket being checked.
    :param bucket: Bucket name to check.
    :param prefix: Folder prefix, without a trailing slash.
    """
    response = s3.get_conn().list_objects_v2(Bucket=bucket, Prefix=prefix + "/", MaxKeys=1)
    return response.get("KeyCount", 0) > 0


def list_prefix_keys(s3: S3Hook, bucket: str, prefix: str) -> List[str]:
    """
    Every object key under a folder prefix.

    :param s3: Hook for the bucket being listed.
    :param bucket: Bucket name to list.
    :param prefix: Folder prefix, without a trailing slash.
    """
    paginator = s3.get_conn().get_paginator("list_objects_v2")
    return [o["Key"]
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix + "/")
            for o in page.get("Contents", [])]


def delete_prefix(s3: S3Hook, bucket: str, prefix: str) -> int:
    """
    Delete every object under a folder prefix, so a re-conversion leaves no stale output behind.

    :param s3: Hook for the bucket being cleaned.
    :param bucket: Bucket name to clean.
    :param prefix: Folder prefix, without a trailing slash.
    """
    client = s3.get_conn()
    keys = [{"Key": k} for k in list_prefix_keys(s3=s3, bucket=bucket, prefix=prefix) if not k.endswith("/")]
    for i in range(0, len(keys), 1000):  # delete_objects caps at 1000 keys per call
        client.delete_objects(Bucket=bucket, Delete={"Objects": keys[i:i + 1000]})
    return len(keys)


def download_study(s3: S3Hook, bucket: str, study: str, local_dir: str) -> int:
    """
    Download every object of a study into `local_dir`, mirroring its sub-structure. Orthanc `.cmp`
    blobs are decompressed back to `.dcm` on the way.

    :param s3: Hook for the source bucket.
    :param bucket: Bucket name holding the study.
    :param study: Study prefix relative to the bucket root, without a trailing slash.
    :param local_dir: Directory to download into.
    """
    client = s3.get_conn()
    prefix = study + "/"
    keys = [k for k in list_prefix_keys(s3=s3, bucket=bucket, prefix=study) if not k.endswith("/")]

    def _download(key: str) -> None:
        file_path = key[len(prefix):]
        raw = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        if key.endswith(".cmp"):
            raw = orthanc_decompress(raw)
            file_path = file_path.removesuffix(".cmp")
        destination = os.path.join(local_dir, file_path)
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        with open(destination, "wb") as dicom:
            dicom.write(raw)

    with ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as executor:
        list(executor.map(_download, keys))
    return len(keys)


def upload_dir(s3: S3Hook, local_dir: str, bucket: str, prefix: str) -> int:
    """
    Upload a directory tree under a folder prefix.

    :param s3: Hook for the destination bucket.
    :param local_dir: Directory to upload.
    :param bucket: Destination bucket name.
    :param prefix: Destination folder prefix, without a trailing slash.
    """
    uploaded = 0
    for root, _, files in os.walk(local_dir):
        for name in files:
            full_path = os.path.join(root, name)
            relative_path = os.path.relpath(full_path, local_dir).replace(os.sep, "/")
            s3.load_file(full_path, key=f"{prefix}/{relative_path}", bucket_name=bucket, replace=True)
            uploaded += 1
    return uploaded


def dcm2niix_path() -> str:
    """Absolute path to the `dcm2niix` binary installed in the task's virtualenv."""
    # The virtualenv's bin directory is not reliably on PATH, and it is the only copy pinned to
    # DCM2NIIX_VERSION, so it wins over anything the environment happens to carry.
    venv_binary = os.path.join(os.path.dirname(sys.executable), "dcm2niix")
    if os.path.exists(venv_binary):
        return venv_binary

    on_path = shutil.which("dcm2niix")
    if on_path:
        return on_path
    raise AirflowFailException("dcm2niix is not installed in the task virtualenv nor on PATH")


def run_dcm2niix(options: List[str], in_dir: str, out_dir: str) -> None:
    """
    Run `dcm2niix` over a folder of DICOMs.

    :param options: Options controlling what is written, e.g. `["-b", "y", "-ba", "y", "-z", "y"]`.
    :param in_dir: Directory holding the DICOMs. Its name becomes the output filename stem.
    :param out_dir: Directory to write to.
    """
    proc = subprocess.run([dcm2niix_path(), *options, "-o", out_dir, in_dir],
                          capture_output=True, text=True, errors="replace", check=False,
                          timeout=DCM2NIIX_TIMEOUT_SECONDS)
    if proc.returncode != 0:
        raise AirflowFailException(f"dcm2niix rc={proc.returncode}: {proc.stderr[-300:]}")


def convert_study(study: str, red_s3: S3Hook, yellow_s3: S3Hook, skip_existing: bool) -> str:
    """
    Convert one study: stage the DICOMs, run both `dcm2niix` passes, upload, then drop the staged copy.

    Two passes are needed because the anonymized and nominative sidecars differ only by `-ba`. The
    anonymizing pass runs first and is the one that produces the NIfTI, so the images published to the
    yellow bucket never originate from a nominative run.

    :param study: Source study prefix relative to the bucket root.
    :param red_s3: Hook for the red bucket, holding the DICOMs and the nominative sidecars.
    :param yellow_s3: Hook for the yellow bucket, holding the NIfTI and anonymized sidecars.
    :param skip_existing: True to leave studies whose NIfTI output already exists untouched. False to
        delete that output and convert again.
    """
    accession_number = study.rstrip("/").split("/")[-1]
    nifti_prefix = get_output_prefix(study=study, parent_prefix=NIFTI_PREFIX)
    sidecar_prefix = get_output_prefix(study=study, parent_prefix=NIFTI_SIDECARS_PREFIX)

    if skip_existing and prefix_has_objects(s3=yellow_s3, bucket=VNA_CLINIQUE_YELLOW_BUCKET, prefix=nifti_prefix):
        return "skipped (output exists)"

    with tempfile.TemporaryDirectory(prefix=f"dcm_{accession_number}_") as work:
        # dcm2niix builds its output filenames from the input folder name, so naming it after the
        # accession number is what makes the results come out as "RA202600030101_*".
        in_dir = os.path.join(work, accession_number)
        anonymized_dir = os.path.join(work, "anonymized")
        nominative_dir = os.path.join(work, "nominative")
        for directory in (in_dir, anonymized_dir, nominative_dir):
            os.makedirs(directory, exist_ok=True)

        if download_study(s3=red_s3, bucket=VNA_CLINIQUE_RED_BUCKET, study=study, local_dir=in_dir) == 0:
            return "WARNING: no input objects"

        # NIfTI + anonymized sidecar
        run_dcm2niix(["-b", "y", "-ba", "y", "-z", "y"], in_dir=in_dir, out_dir=anonymized_dir)
        # nominative sidecar only
        run_dcm2niix(["-b", "o", "-ba", "n"], in_dir=in_dir, out_dir=nominative_dir)

        if not os.listdir(anonymized_dir):
            return "WARNING: no NIfTI produced"

        if not skip_existing:
            delete_prefix(s3=yellow_s3, bucket=VNA_CLINIQUE_YELLOW_BUCKET, prefix=nifti_prefix)
            delete_prefix(s3=red_s3, bucket=VNA_CLINIQUE_RED_BUCKET, prefix=sidecar_prefix)

        nifti_count = upload_dir(s3=yellow_s3, local_dir=anonymized_dir,
                                 bucket=VNA_CLINIQUE_YELLOW_BUCKET, prefix=nifti_prefix)
        sidecar_count = upload_dir(s3=red_s3, local_dir=nominative_dir,
                                   bucket=VNA_CLINIQUE_RED_BUCKET, prefix=sidecar_prefix)

    return f"ok ({nifti_count} files, {sidecar_count} nominative sidecars)"
