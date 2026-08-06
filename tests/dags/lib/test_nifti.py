# pylint: disable=invalid-name
"""Unit tests for the pure helpers in lib.nifti.

Covers Orthanc decompression, exam date parsing, source-to-output prefix mapping, the study folder
walk including wildcards, accession file parsing, and the run report.
"""
import csv
import io
import zlib
from datetime import date
from unittest.mock import MagicMock

import pytest
from airflow.exceptions import AirflowFailException

from lib.config import NIFTI_PREFIX, NIFTI_SIDECARS_PREFIX
from lib.nifti import (
    DICOM_MAGIC,
    DICOM_PREAMBLE_LEN,
    ORTHANC_HEADER_LEN,
    ConversionStatus,
    build_report_csv,
    conversion_result,
    dcm2niix_output,
    get_output_prefix,
    is_report_only,
    list_study_folders,
    orthanc_decompress,
    parse_exam_date,
    read_accession_patterns,
    report_key,
    report_row,
    sanitize_dcm2niix_output,
    study_pattern,
)

BUCKET = "vna-clinique-red"


def orthanc_blob(payload: bytes = b"pixels") -> bytes:
    """A valid Orthanc `.cmp` blob wrapping a minimal DICOM Part 10 file."""
    dicom = b"\x00" * DICOM_PREAMBLE_LEN + DICOM_MAGIC + payload
    return b"\x00" * ORTHANC_HEADER_LEN + zlib.compress(dicom)


def s3_stub(folders):
    """
    S3Hook stub whose list_objects_v2 paginator walks an in-memory folder tree.

    :param folders: Leaf study folder paths, e.g. ["dicoms/2026/01/02/RA202600030101"].
    """
    def _paginate(Bucket=None, Prefix="", Delimiter=None):  # boto3 kwargs are capitalized
        assert Bucket == BUCKET
        assert Delimiter == "/"
        depth = len(Prefix.rstrip("/").split("/")) if Prefix.strip("/") else 0
        children = sorted({"/".join(f.split("/")[:depth + 1]) + "/"
                           for f in folders if f.startswith(Prefix)})
        return [{"CommonPrefixes": [{"Prefix": c} for c in children]}]

    s3 = MagicMock()
    s3.get_conn.return_value.get_paginator.return_value.paginate.side_effect = _paginate
    return s3


def test_orthanc_decompress_returns_dicom_bytes():
    """It should strip the Orthanc header and inflate to a DICOM Part 10 blob."""
    assert orthanc_decompress(orthanc_blob()).endswith(DICOM_MAGIC + b"pixels")


def test_orthanc_decompress_rejects_non_dicom():
    """It should reject a blob whose decompressed content has no DICM marker."""
    blob = b"\x00" * ORTHANC_HEADER_LEN + zlib.compress(b"not a dicom file")
    with pytest.raises(ValueError):
        orthanc_decompress(blob)


def test_parse_exam_date_accepts_iso_8601():
    assert parse_exam_date("2026-01-15") == date(2026, 1, 15)


def test_parse_exam_date_strips_surrounding_whitespace():
    """It should tolerate padded cells, which CSV exports routinely carry."""
    assert parse_exam_date(" 2026-01-15 ") == date(2026, 1, 15)


# `20260115` is rejected on purpose: date.fromisoformat would accept it on Python 3.11+ only, so the
# format is pinned to keep parsing identical across interpreter versions.
@pytest.mark.parametrize("value", ["03/04/2026", "15-01-2026", "Jan 15 2026", "20260115", ""])
def test_parse_exam_date_rejects_everything_else(value):
    """It should reject ambiguous or non-ISO dates rather than guess the day and month order."""
    with pytest.raises(AirflowFailException):
        parse_exam_date(value)


def test_study_pattern_builds_dated_prefix():
    assert study_pattern(exam_date=date(2026, 1, 2), accession_number="RA2026000*") == \
           "dicoms/2026/01/02/RA2026000*"


@pytest.mark.parametrize("parent_prefix, expected", [
    (NIFTI_PREFIX, "nifti/2026/01/02/RA202600030101"),
    (NIFTI_SIDECARS_PREFIX, "nifti_sidecars/2026/01/02/RA202600030101"),
])
def test_get_output_prefix_preserves_date_structure(parent_prefix, expected):
    assert get_output_prefix(study="dicoms/2026/01/02/RA202600030101",
                             parent_prefix=parent_prefix) == expected


def test_get_output_prefix_rejects_bare_study():
    """It should refuse a study with no parent prefix, which would silently drop a path segment."""
    with pytest.raises(ValueError):
        get_output_prefix(study="RA202600030101", parent_prefix=NIFTI_PREFIX)


def test_list_study_folders_returns_exact_study_without_walking():
    """It should short-circuit on a wildcard-free study prefix rather than list the bucket."""
    s3 = s3_stub([])
    assert list_study_folders(s3=s3, bucket=BUCKET, pattern="dicoms/2026/01/02/RA202600030101") == \
           ["dicoms/2026/01/02/RA202600030101"]
    s3.get_conn.assert_not_called()


@pytest.mark.parametrize("pattern, expected", [
    # a date prefix walks down to every study below it
    ("dicoms/2026/01", ["dicoms/2026/01/02/RA202600030101", "dicoms/2026/01/02/RA202600030102",
                        "dicoms/2026/01/03/XY202600040101"]),
    # wildcards match on the full path, at any depth
    ("dicoms/2026/01/02/RA2026000301*", ["dicoms/2026/01/02/RA202600030101",
                                         "dicoms/2026/01/02/RA202600030102"]),
    ("dicoms/2026/*/*/XY*", ["dicoms/2026/01/03/XY202600040101"]),
    ("dicoms/2026/01/02/ZZ*", []),
])
def test_list_study_folders_walks_and_matches(pattern, expected):
    folders = ["dicoms/2026/01/02/RA202600030101",
               "dicoms/2026/01/02/RA202600030102",
               "dicoms/2026/01/03/XY202600040101"]
    assert list_study_folders(s3=s3_stub(folders), bucket=BUCKET, pattern=pattern) == expected


def accession_file_stub(csv_content: str):
    """S3Hook stub returning `csv_content` as the body of the accession number file."""
    body = MagicMock()
    body.read.return_value = csv_content.encode()
    s3 = MagicMock()
    s3.get_key.return_value.get.return_value = {'Body': body}
    return s3


def test_read_accession_patterns_builds_dated_prefixes():
    csv_content = "accessionNumber,examDate\nRA202600012345,2026-01-01\nRA2026000*,2026-02-03\n"
    patterns = read_accession_patterns(s3=accession_file_stub(csv_content), bucket=BUCKET,
                                       key="lists/cohort.csv",
                                       accession_number_column="accessionNumber", exam_date_column="examDate")
    assert patterns == ["dicoms/2026/01/01/RA202600012345", "dicoms/2026/02/03/RA2026000*"]


def test_read_accession_patterns_reports_missing_columns():
    """It should name the missing columns and what the file actually holds."""
    csv_content = "accession,date\nRA202600012345,2026-01-01\n"
    with pytest.raises(AirflowFailException, match="accessionNumber"):
        read_accession_patterns(s3=accession_file_stub(csv_content), bucket=BUCKET,
                                key="lists/cohort.csv",
                                accession_number_column="accessionNumber", exam_date_column="examDate")


# A verbatim excerpt of dcm2niix output, including the long UID filenames that get trimmed out of the
# report.
DCM2NIIX_OUTPUT = (
    "Found 1772 DICOM file(s)\n"
    "Skipping non-image DICOM: /tmp/dcm_RA1_x9/RA1/IRM cérébrale C-/AX T2 TSE/"
    "1.3.46.670589.11.70851.5.24.5.1.5132.2018022808544539710-45f6b2d4-3587-4c9b-9168-dd7dbe755592.dcm\n"
    "Convert 160 DICOM as /tmp/dcm_RA1_x9/anonymized/RA1_eeSAG_T1_3D_TFE_FILTRE_20180228085211_502 "
    "(288x288x160x1)\n"
    "Error: Converted 886 of 1772 files\n"
)


def test_sanitize_drops_the_uid_filename_but_keeps_the_series():
    """It should leave the series path readable while removing the DICOM instance identifier."""
    clean = sanitize_dcm2niix_output(DCM2NIIX_OUTPUT, work_dir="/tmp/dcm_RA1_x9")

    assert "IRM cérébrale C-/AX T2 TSE" in clean          # which series was skipped is still visible
    assert "1.3.46.670589" not in clean                   # SOP Instance UID gone
    assert "45f6b2d4-3587-4c9b" not in clean              # and the Orthanc id with it
    assert "/tmp/dcm_RA1_x9" not in clean                 # staging path stripped
    assert "Error: Converted 886 of 1772 files" in clean   # the diagnosis survives


def test_sanitize_leaves_output_without_identifiers_alone():
    assert sanitize_dcm2niix_output("Found 12 DICOM file(s)") == "Found 12 DICOM file(s)"


def test_dcm2niix_output_joins_both_streams():
    """It should read stdout too, since dcm2niix writes its errors there rather than to stderr."""
    proc = MagicMock(stdout="Error: Converted 886 of 1772 files\n", stderr="")
    assert dcm2niix_output(proc) == "Error: Converted 886 of 1772 files"

    proc = MagicMock(stdout="on stdout", stderr="on stderr")
    assert dcm2niix_output(proc) == "on stdout\non stderr"


def stage(tmp_path, *relative_paths):
    """Create empty staged files at the given paths under tmp_path."""
    for relative in relative_paths:
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"")
    return str(tmp_path)


def test_is_report_only_detects_a_study_holding_just_a_report(tmp_path):
    staged = stage(tmp_path, "IRM cérébrale C-/FUJI Basic Text SR for HL7 Radiological Report/a.dcm")
    assert is_report_only(staged) is True


@pytest.mark.parametrize("relative_paths", [
    ("IRM cérébrale C-/AX T2 TSE/a.dcm",),                                    # images only
    ("IRM cérébrale C-/Basic Text SR/a.dcm", "IRM cérébrale C-/AX T2/b.dcm"),  # images alongside
])
def test_is_report_only_false_when_image_series_present(tmp_path, relative_paths):
    assert is_report_only(stage(tmp_path, *relative_paths)) is False


def test_is_report_only_false_for_an_empty_directory(tmp_path):
    assert is_report_only(str(tmp_path)) is False


def test_report_key_names_the_attempt():
    """It should keep one key per attempt, since a re-run reports a smaller set than the first."""
    assert report_key("20260731T173459_try1") == \
           "nifti_reports/20260731T173459_try1_conversion_report.csv"
    assert report_key("20260731T173459_try1") != report_key("20260731T173459_try2")


def test_report_row_splits_the_path():
    result = conversion_result(ConversionStatus.PARTIAL, exit_code=8, files_converted=19, uploaded=True,
                              output="Error: Converted 886 of 1772 files")
    row = report_row("dicoms/2018/02/28/RA201801877901", result)

    assert row["accession"] == "RA201801877901"
    assert row["exam_date"] == "2018-02-28"
    assert row["path"] == "dicoms/2018/02/28/RA201801877901"
    assert row["status"] == ConversionStatus.PARTIAL
    assert row["exit_code"] == 8
    assert row["uploaded"] == "yes"
    assert row["files_converted"] == 19


def test_report_row_blanks_a_missing_exit_code():
    row = report_row("dicoms/2018/01/19/RA201800539901", conversion_result(ConversionStatus.MISSING))
    assert row["exit_code"] == ""
    assert row["uploaded"] == "no"


def test_build_report_csv_round_trips_multiline_output():
    """It should quote the multi-line dcm2niix output so the CSV survives a reader."""
    row = report_row("dicoms/2018/02/28/RA201801877901",
                     conversion_result(ConversionStatus.PARTIAL, exit_code=8, uploaded=True,
                                       output=sanitize_dcm2niix_output(DCM2NIIX_OUTPUT, "/tmp/dcm_RA1_x9")))
    parsed = list(csv.DictReader(io.StringIO(build_report_csv([row]))))

    assert len(parsed) == 1
    assert parsed[0]["accession"] == "RA201801877901"
    assert "Error: Converted 886 of 1772 files" in parsed[0]["output"]
    assert "1.3.46.670589" not in parsed[0]["output"]


def test_build_report_csv_writes_a_header_when_there_is_nothing_to_report():
    assert build_report_csv([]).strip() == ",".join(
        ["accession", "exam_date", "path", "status", "exit_code", "uploaded", "files_converted", "output"])
