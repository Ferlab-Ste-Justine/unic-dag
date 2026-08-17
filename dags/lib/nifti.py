# pylint: disable=too-many-locals
"""
DICOM to NIfTI conversion for the VNA clinique imaging buckets.

Source DICOMs are Orthanc-compressed (`.cmp`) and laid out as
`dicoms/<year>/<month>/<day>/<accession number>/`. Outputs keep that date structure under one prefix
per representation: `nifti/` in the yellow bucket and `nifti_sidecars/` in the red one.
"""
import csv
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
from enum import Enum
from typing import Dict, List, Optional

import pandas as pd
from airflow.exceptions import AirflowFailException
from airflow.models import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.config import Config

from lib.config import MINIO_CONN_ID, NIFTI_PREFIX, NIFTI_REPORTS_PREFIX, NIFTI_SIDECARS_PREFIX, \
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

# The two dcm2niix exit codes worth handling. Any other non-zero code is a genuine failure.
DCM2NIIX_RC_NO_IMAGES = 2  # nothing convertible in the study
DCM2NIIX_RC_PARTIAL = 8  # some series converted, others were skipped


class ConversionStatus(str, Enum):
    """
    How a study's conversion ended. Declaration order is the order the run summary reports them, best
    outcome first.
    """
    OK = "ok"
    PARTIAL = "partial"
    REPORT_ONLY = "report only"
    SKIPPED = "skipped"
    MISSING = "missing"
    FAILED = "failed"

    def __str__(self) -> str:
        # So a log line or a CSV cell gets the value rather than the member name. Without this, `%s`
        # renders "ConversionStatus.PARTIAL", and the f-string result differs between Python 3.9 and
        # the 3.12 that runs in prod.
        return self.value


# Statuses worth reporting. OK and SKIPPED are the uneventful ones.
PROBLEM_STATUSES = [ConversionStatus.PARTIAL, ConversionStatus.REPORT_ONLY,
                    ConversionStatus.MISSING, ConversionStatus.FAILED]

# A series whose path carries one of these holds a report rather than images. Used to confirm that a
# dcm2niix "no images" exit really is a report-only study. A presentation state carries annotations
# and display parameters, never pixel data.
REPORT_MARKERS = ["sr for hl7", "basic text sr", "structured report", "radiological report",
                  "rapport", "dose report", "presentation state"]

# dcm2niix quotes the full path of every file it skips, and a DICOM filename is a ~130 character UID.
# A study can skip a dozen of them, so the names are trimmed to keep the report's output column
# readable. The series path around them is what says which series was skipped.
DICOM_FILENAME_RE = re.compile(r"[^/\s]+\.dcm(?:\.cmp)?\b")
UID_RE = re.compile(r"\b\d+(?:\.\d+){4,}\b")

REPORT_COLUMNS = ["accession", "exam_date", "path", "status", "exit_code", "uploaded",
                  "files_converted", "output"]


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


def study_pattern(exam_date: date, accession_number: str, parent_prefix: str) -> str:
    """
    Build a study prefix from an exam date and an accession number.

    :param exam_date: Date of the exam.
    :param accession_number: Accession number, may contain `*` wildcards.
    :param parent_prefix: Prefix the studies live under, one per representation.
    """
    return f"{parent_prefix}/{exam_date:%Y/%m/%d}/{accession_number}"


def study_selection_params(parent_prefix: str, action: str) -> Dict[str, Param]:
    """
    Params picking which studies a DAG acts on, either as prefixes or from an accession number file.

    :param parent_prefix: Prefix the studies live under, one per representation.
    :param action: Verb naming what the DAG does to a study, used in the descriptions.
    """
    return {
        "paths": Param([], type=["null", "array"],
                       description=f"Study prefixes to {action}, one per line. Wildcards allowed in any "
                                   f"segment. Ex: {parent_prefix}/2026/01/01/RA202600012345, "
                                   f"{parent_prefix}/2026/01/01/RA2026000*, {parent_prefix}/2026/01/*. "
                                   f"Mutually exclusive with 'accession_file_key'."),
        "accession_file_bucket": Param(None, type=["null", "string"],
                                       description="(Optional) Bucket holding the accession number CSV file. Required with 'accession_file_key'."),
        "accession_file_key": Param(None, type=["null", "string"],
                                    description="(Optional) Key of the accession number CSV file. Required with 'accession_file_bucket'. Mutually exclusive with 'paths'."),
        "accession_number_column": Param("accessionNumber", type="string",
                                         description="Accession number column in the CSV file. Wildcards allowed in values."),
        "exam_date_column": Param("examDate", type="string",
                                  description="Exam date column in the CSV file. Must be ISO 8601. Ex: 2026-01-15"),
    }


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


def read_accession_patterns(s3: S3Hook, bucket: str, key: str, accession_number_column: str,
                            exam_date_column: str, parent_prefix: str) -> List[str]:
    """
    Read a CSV of accession numbers and exam dates into a list of study prefixes.

    :param s3: Hook for the bucket holding the file.
    :param bucket: Bucket name of the CSV file.
    :param key: Key of the CSV file.
    :param accession_number_column: Column holding the accession numbers, wildcards allowed in values.
    :param exam_date_column: Column holding the exam dates, `YYYY-MM-DD` only.
    :param parent_prefix: Prefix the studies live under, one per representation.
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
                          accession_number=str(row[accession_number_column]).strip(),
                          parent_prefix=parent_prefix)
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
    :return: Number of objects deleted.
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
    :return: Number of objects downloaded. Zero means the study prefix holds nothing.
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
    :return: Number of files uploaded.
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


def run_dcm2niix(options: List[str], in_dir: str, out_dir: str) -> subprocess.CompletedProcess:
    """
    Run `dcm2niix` over a folder of DICOMs and hand the completed process back to the caller.

    The exit code is deliberately not raised on: dcm2niix reports both a partial conversion and a study
    with no convertible images through it, and each needs a different outcome.

    :param options: Options controlling what is written, e.g. `["-b", "y", "-ba", "y", "-z", "y"]`.
    :param in_dir: Directory holding the DICOMs. Its name becomes the output filename stem.
    :param out_dir: Directory to write to.
    """
    return subprocess.run([dcm2niix_path(), *options, "-o", out_dir, in_dir],
                          capture_output=True, text=True, errors="replace", check=False,
                          timeout=DCM2NIIX_TIMEOUT_SECONDS)


def dcm2niix_output(proc: subprocess.CompletedProcess) -> str:
    """
    Both output streams of a dcm2niix run, joined.

    dcm2niix writes nearly everything to stdout, including the errors, so reading stderr alone leaves
    the reason for a failure empty.

    :param proc: A completed `run_dcm2niix` call.
    """
    return "\n".join(stream.strip() for stream in (proc.stdout, proc.stderr) if stream and stream.strip())


def sanitize_dcm2niix_output(text: str, work_dir: str = "") -> str:
    """
    Trim the noise out of dcm2niix output so it reads in a CSV cell.

    Only the DICOM filenames and the staging path go: the series path around them is kept, since it is
    what says which series was skipped.

    :param text: Raw dcm2niix output.
    :param work_dir: Staging directory to strip from the paths, so they read relative to the study.
    """
    if work_dir:
        text = text.replace(work_dir.rstrip(os.sep) + os.sep, "")
    return UID_RE.sub("<uid>", DICOM_FILENAME_RE.sub("<file>", text))


def is_report_only(local_dir: str) -> bool:
    """
    Whether every staged file sits in a series whose path marks it as a report or other non-image
    object rather than images.

    :param local_dir: Directory holding the staged DICOMs.
    """
    staged = [os.path.join(root, name) for root, _, names in os.walk(local_dir) for name in names]
    if not staged:
        return False
    return all(any(marker in path.lower() for marker in REPORT_MARKERS) for path in staged)


def conversion_result(status: ConversionStatus, exit_code: Optional[int] = None, files_converted: int = 0,
                      uploaded: bool = False, output: str = "", detail: str = "") -> dict:
    """
    One study's outcome, as consumed by the progress log and the run report.

    :param status: How the conversion ended.
    :param exit_code: dcm2niix exit code, when it got as far as running.
    :param files_converted: Number of files uploaded to the yellow bucket.
    :param uploaded: Whether anything was written to the destination.
    :param output: Sanitized dcm2niix output, kept for any non-zero exit code.
    :param detail: Short phrase for the progress line.
    """
    return {"status": status, "exit_code": exit_code, "files_converted": files_converted,
            "uploaded": uploaded, "output": output, "detail": detail}


def convert_study(study: str, red_s3: S3Hook, yellow_s3: S3Hook, skip_existing: bool) -> dict:
    """
    Convert one study: stage the DICOMs, run both `dcm2niix` passes, upload, then drop the staged copy.

    Two passes are needed because the anonymized and nominative sidecars differ only by `-ba`. The
    anonymizing pass runs first and is the one that produces the NIfTI, so the images published to the
    yellow bucket never originate from a nominative run.

    A partial conversion is still uploaded: dcm2niix exits non-zero when it skips localizers, derived
    reformats, or non-image objects, which says nothing about the anatomical series it did convert.

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
        return conversion_result(ConversionStatus.SKIPPED, detail="output exists")

    with tempfile.TemporaryDirectory(prefix=f"dcm_{accession_number}_") as work:
        # dcm2niix builds its output filenames from the input folder name, so naming it after the
        # accession number is what makes the results come out as "RA202600030101_*".
        in_dir = os.path.join(work, accession_number)
        anonymized_dir = os.path.join(work, "anonymized")
        nominative_dir = os.path.join(work, "nominative")
        for directory in (in_dir, anonymized_dir, nominative_dir):
            os.makedirs(directory, exist_ok=True)

        if download_study(s3=red_s3, bucket=VNA_CLINIQUE_RED_BUCKET, study=study, local_dir=in_dir) == 0:
            return conversion_result(ConversionStatus.MISSING, detail="no input objects")

        # NIfTI + anonymized sidecar
        anonymized = run_dcm2niix(["-b", "y", "-ba", "y", "-z", "y"], in_dir=in_dir, out_dir=anonymized_dir)
        exit_code = anonymized.returncode
        output = sanitize_dcm2niix_output(dcm2niix_output(anonymized), work)

        if exit_code == DCM2NIIX_RC_NO_IMAGES:
            # Confirmed against the staged files rather than trusted: a study holding image series that
            # dcm2niix refused is a different problem from one holding only a report.
            if not is_report_only(in_dir):
                raise AirflowFailException(
                    f"dcm2niix rc={exit_code} but the study holds image series: {output[-300:]}")
            return conversion_result(ConversionStatus.REPORT_ONLY, exit_code=exit_code, output=output,
                                     detail="no image series to convert")

        if exit_code not in (0, DCM2NIIX_RC_PARTIAL):
            raise AirflowFailException(f"dcm2niix rc={exit_code}: {output[-300:]}")

        # The nominative pass sees the same DICOMs, so it exits the same way. Its code is not checked
        # again: failing here would discard the output the first pass already earned.
        run_dcm2niix(["-b", "o", "-ba", "n"], in_dir=in_dir, out_dir=nominative_dir)

        if not os.listdir(anonymized_dir):
            raise AirflowFailException(f"dcm2niix rc={exit_code} produced no output: {output[-300:]}")

        if not skip_existing:
            delete_prefix(s3=yellow_s3, bucket=VNA_CLINIQUE_YELLOW_BUCKET, prefix=nifti_prefix)
            delete_prefix(s3=red_s3, bucket=VNA_CLINIQUE_RED_BUCKET, prefix=sidecar_prefix)

        nifti_count = upload_dir(s3=yellow_s3, local_dir=anonymized_dir,
                                 bucket=VNA_CLINIQUE_YELLOW_BUCKET, prefix=nifti_prefix)
        sidecar_count = upload_dir(s3=red_s3, local_dir=nominative_dir,
                                   bucket=VNA_CLINIQUE_RED_BUCKET, prefix=sidecar_prefix)

    partial = exit_code == DCM2NIIX_RC_PARTIAL
    return conversion_result(ConversionStatus.PARTIAL if partial else ConversionStatus.OK,
                             exit_code=exit_code, files_converted=nifti_count, uploaded=True,
                             output=output if partial else "",
                             detail=f"{nifti_count} files, {sidecar_count} nominative sidecars")


def report_key(run_stamp: str) -> str:
    """
    Key of a run's report in the yellow bucket.

    :param run_stamp: Identifier of the attempt. It has to distinguish attempts, not just runs: a
        re-run skips whatever the previous attempt already converted, so its report is a smaller set
        rather than a superset, and overwriting would erase the record of what was converted.
    """
    return f"{NIFTI_REPORTS_PREFIX}/{run_stamp}_conversion_report.csv"


def report_row(study: str, result: dict) -> dict:
    """
    One study's report line.

    :param study: Source study prefix relative to the bucket root.
    :param result: Its `conversion_result`.
    """
    parts = study.rstrip("/").split("/")
    return {"accession": parts[-1],
            "exam_date": "-".join(parts[1:4]) if len(parts) >= 5 else "",
            "path": study,
            "status": result["status"],
            "exit_code": "" if result["exit_code"] is None else result["exit_code"],
            "uploaded": "yes" if result["uploaded"] else "no",
            "files_converted": result["files_converted"],
            "output": result["output"]}


def build_report_csv(rows: List[dict]) -> str:
    """
    Render report rows as CSV. dcm2niix output is multi-line, which the csv module quotes for us.

    :param rows: Rows from `report_row`.
    """
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=REPORT_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()
