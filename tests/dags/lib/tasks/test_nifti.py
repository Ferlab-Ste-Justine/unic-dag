# pylint: disable=invalid-name, unused-argument
"""Unit tests for lib.tasks.nifti.

Covers how the DAG params resolve to study prefixes: the mutual exclusion rules, the bucket and
prefix being the caller's, and the existence check that keeps studies which are not converted yet
from reaching the transfer.
"""
import logging
from unittest.mock import MagicMock

import pytest
from airflow.exceptions import AirflowFailException

from lib.config import DICOM_PREFIX, NIFTI_PREFIX, VNA_CLINIQUE_YELLOW_BUCKET
from lib.tasks import nifti as nifti_tasks

BUCKET = VNA_CLINIQUE_YELLOW_BUCKET
STUDY_A = f"{NIFTI_PREFIX}/2026/01/02/RA202600030101"
STUDY_B = f"{NIFTI_PREFIX}/2026/01/03/RA202600030144"


def params(**overrides):
    """DAG params holding every key resolve_studies reads, defaulted as study_selection_params does."""
    return {
        "paths": [],
        "accession_file_bucket": None,
        "accession_file_key": None,
        "accession_number_column": "accessionNumber",
        "exam_date_column": "examDate",
        **overrides,
    }


def resolve(**kwargs):
    """Call the task's underlying function, bypassing the TaskFlow decorator."""
    return nifti_tasks.resolve_studies.function(**kwargs)


@pytest.fixture(name="stub_s3")
def stub_s3_fixture(monkeypatch):
    """Stub the hook so no Airflow connection is looked up."""
    monkeypatch.setattr(nifti_tasks, "vna_s3_hook", lambda bucket: MagicMock())


@pytest.fixture(name="every_pattern_resolves")
def every_pattern_resolves_fixture(monkeypatch):
    """Make each pattern resolve to itself, as the exact-prefix short-circuit does."""
    monkeypatch.setattr(nifti_tasks, "list_study_folders", lambda s3, bucket, pattern: [pattern])


@pytest.mark.parametrize("overrides, message", [
    ({"accession_file_bucket": BUCKET}, "must be provided together"),
    ({"accession_file_key": "lists/cohort.csv"}, "must be provided together"),
    ({"paths": [STUDY_A], "accession_file_bucket": BUCKET, "accession_file_key": "lists/cohort.csv"},
     "mutually exclusive"),
    ({}, "is required"),
])
def test_resolve_studies_rejects_bad_param_combinations(stub_s3, overrides, message):
    with pytest.raises(AirflowFailException, match=message):
        resolve(bucket=BUCKET, parent_prefix=NIFTI_PREFIX, params=params(**overrides))


def test_resolve_studies_walks_the_bucket_it_is_given(stub_s3, monkeypatch):
    """Both DAGs share the task, so the bucket walked has to come from the caller."""
    walked = []

    def _list(s3, bucket, pattern):
        walked.append(bucket)
        return [pattern]

    monkeypatch.setattr(nifti_tasks, "list_study_folders", _list)

    assert resolve(bucket=BUCKET, parent_prefix=NIFTI_PREFIX,
                   params=params(paths=[STUDY_A])) == [STUDY_A]
    assert walked == [BUCKET]


def test_resolve_studies_forwards_the_parent_prefix_to_the_accession_file(stub_s3, monkeypatch,
                                                                         every_pattern_resolves):
    """One cohort file addresses the NIfTI outputs for a transfer and the DICOMs for a conversion."""
    seen = {}

    def _read(parent_prefix, **_):
        seen["parent_prefix"] = parent_prefix
        return [STUDY_A]

    monkeypatch.setattr(nifti_tasks, "read_accession_patterns", _read)

    resolve(bucket=BUCKET, parent_prefix=NIFTI_PREFIX,
            params=params(accession_file_bucket=BUCKET, accession_file_key="lists/cohort.csv"))
    assert seen["parent_prefix"] == NIFTI_PREFIX


def test_resolve_studies_dedupes_while_preserving_order(stub_s3, every_pattern_resolves):
    assert resolve(bucket=BUCKET, parent_prefix=NIFTI_PREFIX,
                   params=params(paths=[STUDY_B, STUDY_A, STUDY_B])) == [STUDY_B, STUDY_A]


def test_resolve_studies_reports_a_pattern_that_matched_nothing(stub_s3, monkeypatch, caplog):
    """A miss is a warning, not a failure: the rest of the cohort still gets through."""
    monkeypatch.setattr(nifti_tasks, "list_study_folders",
                        lambda s3, bucket, pattern: [STUDY_A] if "*" in pattern else [])

    with caplog.at_level(logging.WARNING):
        studies = resolve(bucket=BUCKET, parent_prefix=NIFTI_PREFIX,
                          params=params(paths=[f"{NIFTI_PREFIX}/2026/01/*", STUDY_B]))

    assert studies == [STUDY_A]
    assert STUDY_B in caplog.text


def test_resolve_studies_drops_prefixes_holding_no_object(stub_s3, monkeypatch,
                                                         every_pattern_resolves, caplog):
    """An RA with no NIfTI yet would make rclone exit 3, so verification has to rule it out."""
    monkeypatch.setattr(nifti_tasks, "prefix_has_objects",
                        lambda s3, bucket, prefix: prefix == STUDY_A)

    with caplog.at_level(logging.WARNING):
        studies = resolve(bucket=BUCKET, parent_prefix=NIFTI_PREFIX, verify_objects=True,
                          params=params(paths=[STUDY_A, STUDY_B]))

    assert studies == [STUDY_A]
    assert STUDY_B in caplog.text


def test_resolve_studies_leaves_prefixes_unverified_by_default(stub_s3, monkeypatch,
                                                              every_pattern_resolves):
    """The conversion DAG reports a missing study in its own CSV, so it must not be dropped here."""
    monkeypatch.setattr(nifti_tasks, "prefix_has_objects",
                        lambda s3, bucket, prefix: pytest.fail("existence should not be checked"))
    study = f"{DICOM_PREFIX}/2026/01/02/RA202600030101"

    assert resolve(bucket=BUCKET, parent_prefix=DICOM_PREFIX, params=params(paths=[study])) == [study]


def test_resolve_studies_fails_when_no_study_exists(stub_s3, monkeypatch, every_pattern_resolves):
    """Nothing to transfer at all is a failure, unlike a cohort that is merely incomplete."""
    monkeypatch.setattr(nifti_tasks, "prefix_has_objects", lambda s3, bucket, prefix: False)

    with pytest.raises(AirflowFailException, match="No study folder matched"):
        resolve(bucket=BUCKET, parent_prefix=NIFTI_PREFIX, verify_objects=True,
                params=params(paths=[STUDY_A, STUDY_B]))
