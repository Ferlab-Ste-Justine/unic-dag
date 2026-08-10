"""
============================================================
                      hl7_io_utils
============================================================

IO helpers for the ``curated_hl7_parsing`` pipeline outputs.

The extracted **tables** are stored as a date-first CSV tree, one CSV per table:

    <tree_base>/<yyyy>/<mm>/<dd>/<hl7_id>/table_<index>.csv      (date from ``dte_of_message``)

**Reports** are a Delta table (markdown), partitioned by ``dte_of_message``.

The tables/markdown tree is foldered by ``hl7_id``; the report Delta is keyed by ``(dte_of_message, hl7_id)``.
Heavy imports are deferred into the functions, so this module is safe to import inside ``@task.virtualenv`` bodies.
"""
from __future__ import annotations

from typing import Literal


# pylint: disable=import-outside-toplevel, import-error, too-many-locals


def build_storage_options(minio_conn_id: str) -> dict:
    """Airflow MinIO connection -> delta-rs / object-store ``storage_options`` (AWS_* keys)."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    conn = S3Hook(aws_conn_id=minio_conn_id).get_connection(minio_conn_id)
    endpoint = conn.extra_dejson.get("endpoint_url") or conn.extra_dejson.get("host") or conn.host
    opts = {
        "AWS_ACCESS_KEY_ID": conn.login,
        "AWS_SECRET_ACCESS_KEY": conn.password,
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        # delta-rs needs this to write to S3-compatible stores without a locking provider (MinIO).
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
    if endpoint:
        opts["AWS_ENDPOINT_URL"] = endpoint
    return opts


# ---- input document helpers ----

def _detect_format(raw: bytes) -> Literal["pdf", "rtf", "other"]:
    """Magic-byte sniff of a decoded document: ``%PDF-`` -> pdf, ``{\\rtf`` -> rtf, else other."""
    if raw[:5] == b"%PDF-":
        return "pdf"
    if raw[:5] == b"{\\rtf":
        return "rtf"
    return "other"


# ---- path / uri helpers ----

def _split_uri(uri: str) -> tuple[str, str]:
    """``s3://bucket/some/prefix`` -> ``("bucket", "some/prefix")`` (no leading/trailing slash)."""
    _, _, remainder = uri.partition("://")
    bucket, _, key = remainder.partition("/")
    return bucket, key.strip("/")


def _sanitize_id(hl7_id: str) -> str:
    """Make a ``hl7_id`` safe to use as a single S3 path segment."""
    import re

    return re.sub(r"[^A-Za-z0-9._]", "_", str(hl7_id))


def _fill_parsing_path(pattern: str, dte_of_message: str, hl7_id: str, table_index: int = None) -> str:
    """Fill the ``{{date}}``/``{{hl7_id}}``/``{{table_no}}`` placeholders stored in a parsing dataset's path.

    Mirror of the unic-etl ``hl7.replaceHL7PathPlaceholders`` util: ``{{date}}`` -> ``yyyy/MM/dd``,
    ``{{hl7_id}}`` -> the sanitized id, ``{{table_no}}`` -> the table index (left untouched when ``table_index``
    is None, e.g. the report.md pattern has no ``{{table_no}}``). Works on a full URI or a bare key alike.
    """
    from datetime import date

    parsed = date.fromisoformat(str(dte_of_message)[:10])  # dte_of_message is yyyy-MM-dd
    filled = (pattern.replace("{{date}}", f"{parsed.year:04d}/{parsed.month:02d}/{parsed.day:02d}")
                     .replace("{{hl7_id}}", _sanitize_id(hl7_id)))
    return filled if table_index is None else filled.replace("{{table_no}}", str(table_index))


def _parsing_date_prefix(pattern: str, dte_of_message: str) -> str:
    """The day folder holding a date's document leaves: the pattern truncated at ``{{hl7_id}}`` with
    ``{{date}}`` filled and no trailing slash, e.g. ``<base>/extracted/<yyyy>/<mm>/<dd>``."""
    from datetime import date

    parsed = date.fromisoformat(str(dte_of_message)[:10])  # dte_of_message is yyyy-MM-dd
    head = pattern.split("{{hl7_id}}")[0]  # e.g. "<base>/extracted/{{date}}/"
    return head.replace("{{date}}", f"{parsed.year:04d}/{parsed.month:02d}/{parsed.day:02d}").rstrip("/")


# ---- tables: write (CSV tree) ----

def write_tables(tables_df, *, tables_pattern_uri: str, minio_conn_id: str) -> None:
    """Write each extracted table to its placeholder-resolved key under the tables dataset.

    :param tables_df: polars frame with ``hl7_id``, ``dte_of_message``, ``table_index``, ``table_csv``.
    :param tables_pattern_uri: s3:// pattern uri of the tables dataset (holds ``{{date}}/{{hl7_id}}/{{table_no}}``).
    :param minio_conn_id: Airflow MinIO connection id.
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    bucket, key_pattern = _split_uri(tables_pattern_uri)
    s3 = S3Hook(aws_conn_id=minio_conn_id)
    for row in tables_df.iter_rows(named=True):
        key = _fill_parsing_path(key_pattern, row["dte_of_message"], row["hl7_id"], row["table_index"])
        # `table_csv` is already the serialized CSV produced by the parser; write it verbatim.
        s3.load_string(string_data=row["table_csv"], key=key, bucket_name=bucket, replace=True)


# ---- reports: write (markdown into the CSV tree) ----

def write_report_markdown_tree(report_df, *, report_md_pattern_uri: str, minio_conn_id: str) -> int:
    """Write each successfully-parsed report's Markdown in the same date-first tree as the extracted tables.
    Rows whose ``report_markdown`` is null are ignored.

    :param report_df: polars frame with ``hl7_id``, ``dte_of_message``, ``report_markdown``.
    :param report_md_pattern_uri: s3:// pattern uri, ends in ``{{date}}/{{hl7_id}}``
    :param minio_conn_id: Airflow MinIO connection id.
    :returns: number of report.md files written.
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    bucket, key_pattern = _split_uri(report_md_pattern_uri)
    s3 = S3Hook(aws_conn_id=minio_conn_id)
    written = 0
    for row in report_df.iter_rows(named=True):
        markdown = row["report_markdown"]
        if markdown is None:
            continue
        key = _fill_parsing_path(key_pattern, row["dte_of_message"], row["hl7_id"])
        s3.load_string(string_data=markdown, key=key, bucket_name=bucket, replace=True)
        written += 1
    return written


# ---- tree: delete (per-date idempotency) ----

def delete_report_tree_for_date(pattern_uri: str, dte_of_message: str, minio_conn_id: str) -> int:
    """Delete every object under a day's hl7 tree folder (the pattern truncated at ``{{hl7_id}}`` with
    ``{{date}}`` filled) so a subsequent per-date write leaves no orphaned leaves from an earlier run.
    Called once per date: tables and report.md share the tree, so the tables pattern's day-folder covers
    both. Returns the key count.
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    bucket, key_pattern = _split_uri(pattern_uri)
    prefix = f"{_parsing_date_prefix(key_pattern, dte_of_message)}/"
    s3 = S3Hook(aws_conn_id=minio_conn_id)
    keys = s3.list_keys(bucket_name=bucket, prefix=prefix) or []
    for start in range(0, len(keys), 1000):  # S3 DeleteObjects caps at 1000 keys per call
        s3.delete_objects(bucket=bucket, keys=keys[start:start + 1000])
    return len(keys)


# ---- reports: write (Delta, window-scoped overwrite) ----

def write_report_delta(report_df, *, report_uri: str, storage_options: dict,
                       window_start: str, window_end: str,
                       partition_col: str = "dte_of_message") -> None:
    """Overwrite the report Delta table for a single half-open window [window_start, window_end).

    - Empty ``report_df`` raises an error.
    - Existing table -> overwrite with a predicate filter which replaces this window only.

    :param report_df: polars frame matching the report schema (partitioned by ``partition_col``).
    :param report_uri: s3:// uri of the report Delta table.
    :param storage_options: delta-rs AWS_* options.
    :param window_start: inclusive window start (yyyy-MM-dd).
    :param window_end: exclusive window end (yyyy-MM-dd).
    :param partition_col: partition column; defaults to ``dte_of_message``.
    :raises AirflowFailException: if ``report_df`` is empty, or on a schema mismatch.
    """
    import logging

    from airflow.exceptions import AirflowFailException
    from deltalake import DeltaTable
    from deltalake.exceptions import DeltaError, SchemaMismatchError

    if report_df.is_empty():
        raise AirflowFailException(
            f"No report rows for window [{window_start}, {window_end}); ")

    write_options: dict[str, object] = {"partition_by": [partition_col]}
    if DeltaTable.is_deltatable(report_uri, storage_options):
        write_options["predicate"] = (
            f"{partition_col} >= '{window_start}' AND {partition_col} < '{window_end}'")

    try:
        report_df.write_delta(report_uri, mode="overwrite", storage_options=storage_options,
                              delta_write_options=write_options)
    except SchemaMismatchError as exc:
        raise AirflowFailException(
            f"Schema mismatch writing report Delta at {report_uri}: {exc}") from exc
    except DeltaError as exc:
        logging.error("Delta write of parsed reports failed for window [%s, %s) at %s: %s",
                      window_start, window_end, report_uri, exc)
        raise
