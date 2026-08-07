"""Unit tests for lib.hl7_io_utils: pure path/uri helpers, the connection->AWS_* mapping, and the
window-scoped report Delta write (empty-frame guard + replaceWhere predicate).

write_report_delta imports polars/deltalake (added to requirements.txt for CI); the path-helper tests
are pure stdlib.
"""
# pylint: disable=protected-access, import-outside-toplevel
import json

import pytest

from lib import hl7_io_utils
from lib.hl7_io_utils import (_detect_format, _fill_parsing_path, _parsing_date_prefix, _sanitize_id,
                              _split_uri, build_storage_options)


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
def test_detect_format(raw, expected):
    assert _detect_format(raw) == expected


def test_fill_parsing_path_tables():
    pattern = "hl7/extracted/{{date}}/{{hl7_id}}/table_{{table_no}}.csv"
    assert _fill_parsing_path(pattern, "2025-08-15", "RAD-00018_1", 2) == \
        "hl7/extracted/2025/08/15/RAD_00018_1/table_2.csv"
    assert _fill_parsing_path(pattern, "2025-08-15T10:00:00", "RAD/00018", 0) == \
        "hl7/extracted/2025/08/15/RAD_00018/table_0.csv"


# report_md has no {{table_no}} -> called with table_index=None, leaving report.md intact;
def test_fill_parsing_path_report_md():
    pattern = "hl7/extracted/{{date}}/{{hl7_id}}/report.md"
    assert _fill_parsing_path(pattern, "2025-08-15", "RAD/00018") == \
        "hl7/extracted/2025/08/15/RAD_00018/report.md"


def test_parsing_date_prefix():
    pattern = "hl7/extracted/{{date}}/{{hl7_id}}/table_{{table_no}}.csv"
    assert _parsing_date_prefix(pattern, "2025-08-15") == "hl7/extracted/2025/08/15"
    assert _parsing_date_prefix(pattern, "2025-08-15T10:00:00") == "hl7/extracted/2025/08/15"


def test_delete_report_tree_for_date(fake_s3_hook):
    # tables and report.md are stored in the same tree, so a single delete of the day folder sweeps both
    day_keys = ["hl7/extracted/2025/08/15/RAD_1/table_0.csv",
                "hl7/extracted/2025/08/15/RAD_1/report.md",
                "hl7/extracted/2025/08/15/RAD_2/table_0.csv"]
    captured = fake_s3_hook(day_keys)

    deleted = hl7_io_utils.delete_report_tree_for_date(
        "s3://red-prd/hl7/extracted/{{date}}/{{hl7_id}}/table_{{table_no}}.csv", "2025-08-15", "redminio")

    assert deleted == 3
    assert captured["conn_id"] == "redminio"
    assert captured["list"] == ("red-prd", "hl7/extracted/2025/08/15/")  # the day prefix, no hl7_id
    assert captured["deleted"] == [("red-prd", day_keys)]             # one delete batch (<1000)


def test_write_report_delta_raises_on_empty_frame():
    import polars as pl
    from airflow.exceptions import AirflowFailException

    empty = pl.DataFrame(schema={"dte_of_message": pl.Utf8, "report_markdown": pl.Utf8})
    with pytest.raises(AirflowFailException):
        hl7_io_utils.write_report_delta(
            empty, report_uri="s3://red-prd/hl7/reports", storage_options={},
            window_start="2025-08-15", window_end="2025-08-16")


def test_write_report_delta_adds_predicate_when_table_exists(fake_write_delta):
    import polars as pl

    captured = fake_write_delta(is_deltatable=True)
    df = pl.DataFrame({"dte_of_message": ["2025-08-15"], "report_markdown": ["x"]})
    hl7_io_utils.write_report_delta(
        df, report_uri="s3://red-prd/hl7/reports", storage_options={},
        window_start="2025-08-15", window_end="2025-08-16")

    assert captured["options"]["partition_by"] == ["dte_of_message"]
    assert captured["options"]["predicate"] == \
        "dte_of_message >= '2025-08-15' AND dte_of_message < '2025-08-16'"


def test_write_report_delta_omits_predicate_when_table_absent(fake_write_delta):
    import polars as pl

    captured = fake_write_delta(is_deltatable=False)
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
