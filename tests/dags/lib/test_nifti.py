# pylint: disable=invalid-name
"""Unit tests for the pure helpers in lib.nifti.

Covers Orthanc decompression, exam date parsing, source-to-output prefix mapping, the study folder
walk including wildcards, and accession file parsing.
"""
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
    get_output_prefix,
    list_study_folders,
    orthanc_decompress,
    parse_exam_date,
    read_accession_patterns,
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


def accession_file_stub(csv: str):
    """S3Hook stub returning `csv` as the body of the accession number file."""
    body = MagicMock()
    body.read.return_value = csv.encode()
    s3 = MagicMock()
    s3.get_key.return_value.get.return_value = {'Body': body}
    return s3


def test_read_accession_patterns_builds_dated_prefixes():
    csv = "accessionNumber,examDate\nRA202600012345,2026-01-01\nRA2026000*,2026-02-03\n"
    patterns = read_accession_patterns(s3=accession_file_stub(csv), bucket=BUCKET, key="lists/cohort.csv",
                                       accession_number_column="accessionNumber", exam_date_column="examDate")
    assert patterns == ["dicoms/2026/01/01/RA202600012345", "dicoms/2026/02/03/RA2026000*"]


def test_read_accession_patterns_reports_missing_columns():
    """It should name the missing columns and what the file actually holds."""
    csv = "accession,date\nRA202600012345,2026-01-01\n"
    with pytest.raises(AirflowFailException, match="accessionNumber"):
        read_accession_patterns(s3=accession_file_stub(csv), bucket=BUCKET, key="lists/cohort.csv",
                                accession_number_column="accessionNumber", exam_date_column="examDate")
