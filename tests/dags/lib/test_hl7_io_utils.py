"""Unit tests for lib.hl7_io_utils: pure path/uri helpers, the connection->AWS_* mapping, and the
window-scoped report Delta write (empty-frame guard + replaceWhere predicate).

write_report_delta imports polars/deltalake (added to requirements.txt for CI); the path-helper tests
are pure stdlib.
"""
# pylint: disable=protected-access, import-outside-toplevel
import json

import pytest

from lib import hl7_io_utils
from lib.hl7_io_utils import (_detect_format, _sanitize_id, _split_uri, _tree_date_prefix, _tree_dir,
                              _tree_key, _tree_report_key, build_storage_options)


@pytest.mark.parametrize("uri, expected", [
    ("s3://red-prd/hl7/reports", ("red-prd", "hl7/reports")),
    ("s3://red-prd/hl7/reports/", ("red-prd", "hl7/reports")),
    ("s3://red-prd", ("red-prd", "")),
])
def test_split_uri(uri, expected):
    assert _split_uri(uri) == expected


@pytest.mark.parametrize("raw, expected", [
    ("RAD-00018_1", "RAD_00018_1"),
    ("a/b c:d", "a_b_c_d"),
    ("UNIC-40271_2", "UNIC_40271_2"),
])
def test_sanitize_id(raw, expected):
    assert _sanitize_id(raw) == expected


@pytest.mark.parametrize("raw, expected", [
    (b"%PDF-1.7\n%\xe2\xe3\xcf\xd3", "pdf"),   # real PDFs start with %PDF-
    (b"%PDF-", "pdf"),                          # exactly the 5 magic bytes
    (b"{\\rtf1\\ansi\\deff0 hi}", "rtf"),       # real RTFs start with {\rtf
    (b"{\\rtf", "rtf"),
    (b"PK\x03\x04zip", "other"),               # docx/zip etc.
    (b"", "other"),                             # empty input
    (b"%PD", "other"),                          # shorter than the 5-byte prefix
])
def test__detect_format(raw, expected):
    assert _detect_format(raw) == expected


def test_tree_dir_and_key_date_first_layout():
    assert _tree_dir("hl7/tables", "2025-08-15", "RAD-00018_1") == \
        "hl7/tables/2025/08/15/RAD_00018_1"
    # dte with a time part is truncated to the date; hl7_id is path-sanitized
    assert _tree_key("hl7/tables", "2025-08-15T10:00:00", "RAD/00018", 2) == \
        "hl7/tables/2025/08/15/RAD_00018/table_2.csv"


# report.md lives in the same document leaf folder as the tables
def test_tree_report_key_layout():
    assert _tree_report_key("hl7/tables", "2025-08-15", "RAD/00018") == \
        "hl7/tables/2025/08/15/RAD_00018/report.md"


# _tree_date_prefix: the day folder (no hl7_id) that delete_report_tree_for_date clears
def test_tree_date_prefix():
    assert _tree_date_prefix("hl7/tables", "2025-08-15") == "hl7/tables/2025/08/15"
    assert _tree_date_prefix("hl7/tables", "2025-08-15T10:00:00") == "hl7/tables/2025/08/15"


def test_delete_report_tree_for_date(monkeypatch):
    import airflow.providers.amazon.aws.hooks.s3 as s3mod

    captured = {}
    day_keys = ["hl7/tables/2025/08/15/RAD_1/table_0.csv",
                "hl7/tables/2025/08/15/RAD_1/report.md",
                "hl7/tables/2025/08/15/RAD_2/report.md"]

    class FakeS3Hook:
        def __init__(self, aws_conn_id=None):
            captured["conn_id"] = aws_conn_id

        def list_keys(self, bucket_name=None, prefix=None):
            captured["list"] = (bucket_name, prefix)
            return list(day_keys)

        def delete_objects(self, bucket=None, keys=None):
            captured.setdefault("deleted", []).append((bucket, list(keys)))

    monkeypatch.setattr(s3mod, "S3Hook", FakeS3Hook)

    deleted = hl7_io_utils.delete_report_tree_for_date(
        "s3://red-prd/hl7/tables", "2025-08-15", "redminio")

    assert deleted == 3
    assert captured["conn_id"] == "redminio"
    assert captured["list"] == ("red-prd", "hl7/tables/2025/08/15/")  # the day prefix, no hl7_id
    assert captured["deleted"] == [("red-prd", day_keys)]             # one delete batch (<1000)


def test_write_report_delta_raises_on_empty_frame():
    import polars as pl
    from airflow.exceptions import AirflowFailException

    empty = pl.DataFrame(schema={"dte_of_message": pl.Utf8, "report_markdown": pl.Utf8})
    with pytest.raises(AirflowFailException):
        hl7_io_utils.write_report_delta(
            empty, report_uri="s3://red-prd/hl7/reports", storage_options={},
            window_start="2025-08-15", window_end="2025-08-16")


def test_write_report_delta_adds_predicate_when_table_exists(monkeypatch):
    import deltalake
    import polars as pl

    monkeypatch.setattr(deltalake.DeltaTable, "is_deltatable",
                        staticmethod(lambda table_uri, storage_options=None: True))
    captured = {}

    def fake_write_delta(*_args, **kwargs):
        captured["options"] = kwargs["delta_write_options"]

    monkeypatch.setattr(pl.DataFrame, "write_delta", fake_write_delta)
    df = pl.DataFrame({"dte_of_message": ["2025-08-15"], "report_markdown": ["x"]})
    hl7_io_utils.write_report_delta(
        df, report_uri="s3://red-prd/hl7/reports", storage_options={},
        window_start="2025-08-15", window_end="2025-08-16")

    assert captured["options"]["partition_by"] == ["dte_of_message"]
    assert captured["options"]["predicate"] == \
        "dte_of_message >= '2025-08-15' AND dte_of_message < '2025-08-16'"


def test_write_report_delta_omits_predicate_when_table_absent(monkeypatch):
    import deltalake
    import polars as pl

    monkeypatch.setattr(deltalake.DeltaTable, "is_deltatable",
                        staticmethod(lambda table_uri, storage_options=None: False))
    captured = {}

    def fake_write_delta(*_args, **kwargs):
        captured["options"] = kwargs["delta_write_options"]

    monkeypatch.setattr(pl.DataFrame, "write_delta", fake_write_delta)
    df = pl.DataFrame({"dte_of_message": ["2025-08-15"], "report_markdown": ["x"]})
    hl7_io_utils.write_report_delta(
        df, report_uri="s3://red-prd/hl7/reports", storage_options={},
        window_start="2025-08-15", window_end="2025-08-16")

    assert "predicate" not in captured["options"]


def test_build_storage_options_maps_connection(monkeypatch):
    monkeypatch.setenv("AIRFLOW_CONN_REDMINIO", json.dumps({
        "conn_type": "aws", "login": "the-key", "password": "the-secret",
        "extra": {"endpoint_url": "http://minio:9000"},
    }))
    opts = build_storage_options("redminio")
    assert opts["AWS_ACCESS_KEY_ID"] == "the-key"
    assert opts["AWS_SECRET_ACCESS_KEY"] == "the-secret"
    assert opts["AWS_ENDPOINT_URL"] == "http://minio:9000"
    assert opts["AWS_ALLOW_HTTP"] == "true"
    assert opts["AWS_S3_ALLOW_UNSAFE_RENAME"] == "true"
    assert opts["AWS_REGION"] == "us-east-1"
