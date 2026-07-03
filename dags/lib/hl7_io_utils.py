"""
============================================================
                      hl7_io_utils
============================================================

IO helpers for the ``curated_hl7_parsing`` pipeline outputs.

The extracted **tables** are stored as a date-first CSV tree, one CSV per table:

    <tree_base>/<yyyy>/<mm>/<dd>/<hl7_id>/table_<index>.csv      (date from ``dte_of_message``)

**Reports** are a Delta table (markdown), partitioned by ``dte_of_message``.

Both tables and reports are retrieved by an exact ``(hl7_id, dte_of_message)`` pair. Heavy imports
are deferred into the functions, so this module is safe to import
inside ``@task.virtualenv`` bodies.
"""
from __future__ import annotations


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


# ---- path / uri helpers ----

def _split_uri(uri: str) -> tuple[str, str]:
    """``s3://bucket/some/prefix`` -> ``("bucket", "some/prefix")`` (no leading/trailing slash)."""
    _, _, remainder = uri.partition("://")
    bucket, _, key = remainder.partition("/")
    return bucket, key.strip("/")


def _sanitize_id(hl7_id: str) -> str:
    """Make an ``hl7_id`` safe to use as a single S3 path segment."""
    import re

    return re.sub(r"[^A-Za-z0-9._-]", "_", str(hl7_id))


def _tree_dir(base_key: str, dte_of_message: str, hl7_id: str) -> str:
    """Directory holding one document's tables: ``<base_key>/<yyyy>/<mm>/<dd>/<sanitized hl7_id>``."""
    from datetime import date

    parsed = date.fromisoformat(str(dte_of_message)[:10])  # dte_of_message is yyyy-MM-dd
    return f"{base_key}/{parsed.year:04d}/{parsed.month:02d}/{parsed.day:02d}/{_sanitize_id(hl7_id)}"


def _tree_key(base_key: str, dte_of_message: str, hl7_id: str, table_index: int) -> str:
    """``<base_key>/<yyyy>/<mm>/<dd>/<sanitized hl7_id>/table_<index>.csv`` (key, no bucket)."""
    return f"{_tree_dir(base_key, dte_of_message, hl7_id)}/table_{table_index}.csv"


def _table_index_from_key(key: str):
    """Parse the ``table_<index>.csv`` leaf of a tree key -> ``int`` (or ``None`` if it doesn't match)."""
    filename = key.rstrip("/").split("/")[-1]
    if not filename.startswith("table_") or not filename.endswith(".csv"):
        return None
    try:
        return int(filename[len("table_"):-len(".csv")])
    except ValueError:
        return None


# ---- tables: write (CSV tree) ----

def write_tables(tables_df, *, tree_base_uri: str, minio_conn_id: str) -> None:
    """Write each extracted table to the date-first CSV tree (one CSV per table row).

    :param tables_df: polars frame with ``hl7_id``, ``dte_of_message``, ``table_index``, ``table_csv``.
    :param tree_base_uri: s3:// root of the CSV tree.
    :param minio_conn_id: Airflow MinIO connection id.
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    bucket, base_key = _split_uri(tree_base_uri)
    s3 = S3Hook(aws_conn_id=minio_conn_id)
    for row in tables_df.iter_rows(named=True):
        key = _tree_key(base_key, row["dte_of_message"], row["hl7_id"], row["table_index"])
        # `table_csv` is already the serialized CSV produced by the parser; write it verbatim.
        s3.load_string(string_data=row["table_csv"], key=key, bucket_name=bucket, replace=True)


# ---- retrieval (exact (hl7_id, dte_of_message) pair) ----

def read_tables_by_id(hl7_id: str, dte_of_message: str, *, tree_base_uri: str, minio_conn_id: str) -> list:
    """Return every extracted table for one ``(hl7_id, dte_of_message)`` key.

    Lists the leaf prefix ``<base>/<yyyy>/<mm>/<dd>/<hl7_id>/`` and reads each CSV with polars.

    :return: list of ``{"hl7_id", "dte_of_message", "table_index", "table": pl.DataFrame}``.
    """
    import io

    import polars as pl
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    bucket, base_key = _split_uri(tree_base_uri)
    prefix = _tree_dir(base_key, dte_of_message, hl7_id) + "/"

    s3 = S3Hook(aws_conn_id=minio_conn_id)
    records = []
    for key in s3.list_keys(bucket_name=bucket, prefix=prefix) or []:
        table_index = _table_index_from_key(key)
        if table_index is None:
            continue
        content = s3.read_key(key, bucket_name=bucket)
        table = pl.read_csv(io.StringIO(content)) if content else pl.DataFrame()
        records.append({"hl7_id": hl7_id, "dte_of_message": dte_of_message,
                        "table_index": table_index, "table": table})
    return records


def read_report(hl7_id: str, dte_of_message: str, *, report_uri: str, minio_conn_id: str) -> list:
    """Return the parsed report row(s) for ``(hl7_id, dte_of_message)`` from the Delta report table.

    :return: list of report rows as dicts (markdown + metadata).
    """
    import polars as pl

    return (pl.scan_delta(report_uri, storage_options=build_storage_options(minio_conn_id))
              .filter((pl.col("hl7_id") == hl7_id) & (pl.col("dte_of_message") == dte_of_message))
              .collect()
              .to_dicts())
