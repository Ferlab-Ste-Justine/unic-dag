"""
============================================================
              hl7_pdf_docling_parsing (task group)
============================================================

TaskGroup that parses the base64-encoded PDF documents stored in the
``observation_value_base64`` column of a curated HL7 OBX Delta table (e.g.
``curated_radimage_hl7_oru_r01_obx``, ``curated_softpath_hl7_oru_r01_obx``) with docling,
and writes back a markdown report (Delta table) + the extracted tables (date-first CSV tree),
with a primary key of the format ``dte_of_message, hl7_id``.

Two ``@task.virtualenv`` consecutive tasks:
  1. ``resolve_input``   — resolve the curated input table URI + MinIO connection from ``DatalakeConfig``;
  2. ``parse_and_write`` — read the run's ``dte_of_message`` interval window, parse with docling, write outputs.
"""
# pylint: disable=import-outside-toplevel, import-error, too-many-locals, too-many-statements, fixme

from airflow.decorators import task, task_group
from kubernetes.client import models as k8s

# Dependencies installed into the parse_and_write venv at task runtime.
# TODO (integration): Docling needs a newer python version, while the Airflow worker env is pinned to 3.8
PARSE_REQUIREMENTS = [
    "docling==2.55.1",
    "polars==1.12.0",
    "deltalake==0.22.3",
    "pyarrow==17.0.0",
]

# parse_and_write runs docling in its own KubernetesExecutor pod.
# executor_config / pod_override is resolved at DAG-parse time and is NOT Jinja-templatable
PARSE_POD_MEMORY = "8Gi"
PARSE_POD_CPU = "2"
PARSE_EXECUTOR_CONFIG = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    # The worker container is named "base" so the override targets it.
                    name="base",
                    resources=k8s.V1ResourceRequirements(
                        requests={"memory": PARSE_POD_MEMORY, "cpu": PARSE_POD_CPU},
                        limits={"memory": PARSE_POD_MEMORY, "cpu": PARSE_POD_CPU},
                    ),
                )
            ]
        )
    )
}


@task.virtualenv(requirements=["pyhocon==0.3.61"], system_site_packages=True)
def resolve_input(resource_code: str, dataset_suffix: str,
                  output_text_path: str, output_tables_tree_path: str) -> dict:
    """
    Builds the curated source id ``curated_{resource_code}_{dataset_suffix}``, looks it up via
    :class:`DatalakeConfig`, and returns its primitive dict:
        {
            "input_uri":    <s3:// uri of the curated OBX table>,
            "minio_conn_id": <connection id chosen for that bucket>,
            "config":        <DatalakeConfig.to_dict() — the populated source/storage>,
            "text_uri":      <s3:// output uri for the parsed-report Delta table>,
            "tables_tree_uri": <s3:// base uri of the extracted-tables CSV tree>,
        }

    :param resource_code: Short resource code, e.g. "radimage" or "softpath".
    :param dataset_suffix: Completes the curated source id; defaults to "hl7_oru_r01_obx".
    :param output_text_path: s3:// uri for the parsed-report Delta output.
    :param output_tables_tree_path: s3:// base uri for the extracted-tables CSV tree.
    :return: Dict consumed by ``parse_and_write``.
    """
    from lib.datalake_config import DatalakeConfig
    from lib.publish_utils import determine_minio_conn_id_from_config

    source_id = f"curated_{resource_code}_{dataset_suffix}"

    # Builds-from-scratch (load + parse) -> this task needs pyhocon + system_site_packages.
    config = DatalakeConfig(sources_id_list={source_id})

    # polars / deltalake read the object store via the s3:// scheme (not s3a://).
    input_uri = config.source_s3_path(source_id, scheme="s3")
    minio_conn_id = determine_minio_conn_id_from_config(
        config.minio_conn_id, config.bucket_for_source(source_id))

    return {
        "input_uri": input_uri,
        "minio_conn_id": minio_conn_id,
        "config": config.to_dict(),
        "text_uri": output_text_path,
        "tables_tree_uri": output_tables_tree_path,
    }


@task.virtualenv(requirements=PARSE_REQUIREMENTS, system_site_packages=True,
                 executor_config=PARSE_EXECUTOR_CONFIG)
def parse_and_write(input_info: dict, interval_start: str, interval_end: str,
                    doc_batch_concurrency: int, enable_ocr: bool) -> dict:
    """
    Read the curated OBX PDFs for the given date range, parse each with docling, and write two
    outputs (keyed by ``hl7_id``): a parsed-report Delta table (markdown), and the extracted tables
    as a date-first CSV tree (one CSV per table) via ``lib.hl7_io_utils.write_tables``.

    :param input_info: Output of ``resolve_input`` (input_uri, minio_conn_id, text/tables uris).
    :param interval_start: Inclusive start of the run's ``dte_of_message`` window (yyyy-MM-dd).
    :param interval_end: Exclusive end of the run's ``dte_of_message`` window (yyyy-MM-dd).
    :param doc_batch_concurrency: docling threaded multi-document concurrency.
    :param enable_ocr: Run OCR (for scanned PDFs). Table-structure detection is always on.
    :return: Small dict of counts for logging.
    """
    import base64
    import logging
    import tempfile

    import polars as pl
    from airflow.exceptions import AirflowFailException

    from lib.docling_utils import build_converter, run
    from lib.hl7_io_utils import build_storage_options, write_tables

    # Explicit column types for the two output frames declared so that:
    # (1) an empty result still yields a correctly-columned frame;
    # (2) all-null columns (parse_error on success, page_no when docling
    # gives no page) keep their type instead of becoming a Null column;
    # (3) every run produces an identical schema, so the Delta table stays schema-stable across overwrites.
    report_schema = {
        "hl7_id": pl.Utf8, "message_id": pl.Utf8, "set_id": pl.Utf8, "dte_of_message": pl.Utf8,
        "report_markdown": pl.Utf8, "source_format": pl.Utf8,
        "parse_status": pl.Utf8, "parse_error": pl.Utf8,
    }
    tables_schema = {
        "hl7_id": pl.Utf8, "message_id": pl.Utf8, "set_id": pl.Utf8, "dte_of_message": pl.Utf8,
        "table_index": pl.Int64, "table_csv": pl.Utf8,
        "n_rows": pl.Int64, "n_cols": pl.Int64, "page_no": pl.Int64,
    }

    # ---- input read: lazy scan + project + date filter, single collect ----
    def read_obx_pdfs(storage_options: dict) -> pl.DataFrame:
        # The half-open window [interval_start, interval_end) must span at least one day.
        if interval_start >= interval_end:
            raise AirflowFailException(
                f"Empty dte_of_message window [{interval_start}, {interval_end}), "
                f"this DAG's schedule interval must be daily or coarser."
            )
        return (
            pl.scan_delta(input_info["input_uri"], storage_options=storage_options)
            .select(["hl7_id", "message_id", "set_id", "observation_value_base64", "dte_of_message"])
            .filter(pl.col("dte_of_message").is_between(
                pl.lit(interval_start), pl.lit(interval_end), closed="left"))
            .collect()
        )

    # ---- decode base64 -> temp files; detect format; skip non-PDF ----
    def detect_format(raw: bytes) -> str:
        if raw[:5] == b"%PDF-":
            return "pdf"
        if raw[:5] == b"{\\rtf":
            return "rtf"
        return "other"

    def materialize_pdfs(df: pl.DataFrame, tmp_dir):
        from pathlib import Path

        pdf_files, meta_by_stem, skipped_rows = [], {}, []
        for i, row in enumerate(df.iter_rows(named=True)):
            keys = {"hl7_id": row["hl7_id"], "message_id": row["message_id"],
                    "set_id": row["set_id"], "dte_of_message": row["dte_of_message"]}
            try:
                raw = base64.b64decode(row["observation_value_base64"])
            except Exception as exc:  # pylint: disable=broad-except
                skipped_rows.append({**keys, "report_markdown": None, "source_format": "other",
                                     "parse_status": "skipped", "parse_error": f"base64 decode: {exc}"})
                continue
            fmt = detect_format(raw)
            if fmt != "pdf":
                skipped_rows.append({**keys, "report_markdown": None, "source_format": fmt,
                                     "parse_status": "skipped",
                                     "parse_error": f"unsupported format: {fmt}"})
                continue
            stem = f"{i:08d}"  # filesystem-safe stem (hl7_id may contain unsafe chars); mapped below
            path = Path(tmp_dir) / f"{stem}.pdf"
            path.write_bytes(raw)
            pdf_files.append(path)
            meta_by_stem[stem] = keys
        return pdf_files, meta_by_stem, skipped_rows

    def table_to_csv(table, document) -> "tuple[str, int, int, int]":
        try:
            tdf = table.export_to_dataframe(document)   # newer docling wants the doc
        except TypeError:
            tdf = table.export_to_dataframe()           # older signature
        page_no = None
        try:
            page_no = table.prov[0].page_no
        except Exception:  # pylint: disable=broad-except
            page_no = None
        return tdf.to_csv(index=False), int(tdf.shape[0]), int(tdf.shape[1]), page_no

    # ---- assemble the two output frames from conversion results ----
    def build_outputs(results, meta_by_stem, skipped_rows):
        report_rows = list(skipped_rows)
        table_rows = []
        for result in results:
            stem = result.input.file.stem
            keys = meta_by_stem.get(stem, {"hl7_id": stem, "message_id": None,
                                           "set_id": None, "dte_of_message": None})
            status = getattr(result.status, "name", str(result.status)).lower()
            if status not in ("success", "partial_success"):
                report_rows.append({**keys, "report_markdown": None, "source_format": "pdf",
                                    "parse_status": status, "parse_error": "docling conversion failed"})
                continue
            document = result.document
            report_rows.append({**keys, "report_markdown": document.export_to_markdown(),
                                "source_format": "pdf", "parse_status": status, "parse_error": None})
            for idx, table in enumerate(document.tables):
                csv, n_rows, n_cols, page_no = table_to_csv(table, document)
                table_rows.append({**keys, "table_index": idx, "table_csv": csv,
                                   "n_rows": n_rows, "n_cols": n_cols, "page_no": page_no})
        return (pl.DataFrame(report_rows, schema=report_schema),
                pl.DataFrame(table_rows, schema=tables_schema))

    # ---- Delta write: overwrite only the processed date range (idempotent re-runs) ----
    def write_delta_overwrite(df: pl.DataFrame, uri: str, storage_options: dict):
        # replaceWhere: atomically replace ONLY this run's window, leaving other windows intact.
        # Half-open [interval_start, interval_end) — must match read_obx_pdfs's filter exactly, else a
        # boundary date owned by the next run could be deleted here without being re-inserted (data loss).
        predicate = f"dte_of_message >= '{interval_start}' AND dte_of_message < '{interval_end}'"
        try:
            df.write_delta(uri, mode="overwrite", storage_options=storage_options,
                           delta_write_options={"partition_by": ["dte_of_message"],
                                                "predicate": predicate})
        except Exception as exc:  # pylint: disable=broad-except
            # First run: no table to replaceWhere against -> create it (partitioned, full overwrite).
            logging.warning("replaceWhere overwrite failed (%s); creating table at %s", exc, uri)
            df.write_delta(uri, mode="overwrite", storage_options=storage_options,
                           delta_write_options={"partition_by": ["dte_of_message"]})

    # ---- orchestration ----
    storage_options = build_storage_options(input_info["minio_conn_id"])
    df = read_obx_pdfs(storage_options)
    logging.info("Read %d OBX rows for [%s, %s)", df.height, interval_start, interval_end)

    with tempfile.TemporaryDirectory() as tmp_dir:
        pdf_files, meta_by_stem, skipped_rows = materialize_pdfs(df, tmp_dir)
        logging.info("Materialized %d PDFs (%d skipped non-PDF/decode)",
                     len(pdf_files), len(skipped_rows))
        converter = build_converter(doc_batch_concurrency, enable_ocr)
        results = run(converter, pdf_files) if pdf_files else []
        report_df, tables_df = build_outputs(results, meta_by_stem, skipped_rows)

    write_delta_overwrite(report_df, input_info["text_uri"], storage_options)
    write_tables(tables_df, tree_base_uri=input_info["tables_tree_uri"],
                 minio_conn_id=input_info["minio_conn_id"])
    logging.info("Wrote %d report rows and %d table rows", report_df.height, tables_df.height)

    return {"rows_read": df.height, "pdfs_parsed": len(pdf_files),
            "skipped": len(skipped_rows), "tables_extracted": tables_df.height}


@task_group(group_id="hl7_pdf_docling_parsing")
def hl7_pdf_docling_parsing(resource_code: str, dataset_suffix: str,
                            output_text_path: str, output_tables_tree_path: str,
                            doc_batch_concurrency: int = 4, enable_ocr: bool = False) -> None:
    """Resolve the curated OBX table, then parse its PDFs and write report + tables.

    The date window is each run's own ``data_interval`` (half-open ``[start, end)``)

    :param resource_code: Short resource code (e.g. "softpath" / "radimage").
    :param dataset_suffix: Completes the curated source id ``curated_<resource_code>_<dataset_suffix>``.
    :param output_text_path: s3:// uri for the parsed-report Delta output.
    :param output_tables_tree_path: s3:// base uri for the extracted-tables CSV tree.
    :param doc_batch_concurrency: docling threaded multi-document concurrency (1 = sequential).
    :param enable_ocr: Run OCR for scanned PDFs (table-structure detection is always on).
    """
    resolved_input = resolve_input(
        resource_code=resource_code,
        dataset_suffix=dataset_suffix,
        output_text_path=output_text_path,
        output_tables_tree_path=output_tables_tree_path,
    )

    parse_and_write(
        input_info=resolved_input,
        interval_start="{{ data_interval_start.in_timezone('America/Montreal').format('YYYY-MM-DD') }}",
        interval_end="{{ data_interval_end.in_timezone('America/Montreal').format('YYYY-MM-DD') }}",
        doc_batch_concurrency=doc_batch_concurrency,
        enable_ocr=enable_ocr,
    )
