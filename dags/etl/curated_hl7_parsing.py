"""
============================================================
                  curated_hl7_parsing
============================================================

Generic pipeline that parses the base64-encoded PDF documents stored in the
``observation_value_base64`` column of curated HL7 OBX Delta tables (e.g.
``curated_radimage_hl7_oru_r01_obx``, ``curated_softpath_hl7_oru_r01_obx``).

For a given resource it:
  1. resolves the curated input table's S3 URI from the datalake config (``resolve_input``);
  2. reads the requested ``dte_of_message`` range, decodes each PDF, parses it with docling, and
     writes back two Delta outputs — markdown report text and extracted tables — keyed by
     ``hl7_id`` (``parse_and_write``, Phase B — not yet implemented).

Manually triggered (``schedule_interval=None``).
"""

from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import Param
from airflow.utils.trigger_rule import TriggerRule
from kubernetes.client import models as k8s

from lib.config import DEFAULT_ARGS
from lib.slack import Slack

DEFAULT_DATASET_SUFFIX = "hl7_oru_r01_obx"

# Dependencies installed into the parse_and_write venv at task runtime.
# NOTE (integration): these pins must be verified against the worker image's Python (3.9) before
# prod use — docling pulls a heavy PyTorch + ML-model stack and version compatibility is strict.
PARSE_REQUIREMENTS = [
    "docling==2.55.1",
    "polars==1.12.0",
    "deltalake==0.22.3",
    "pyarrow==17.0.0",
]

# parse_and_write runs docling (PyTorch + multi-GB models) in its own KubernetesExecutor pod.
# executor_config / pod_override is resolved at DAG-parse time and is NOT Jinja-templatable, so the
# pod size is a hardcoded constant here (change via PR). One shared model copy + threaded
# doc_batch_concurrency keeps this modest; bump if concurrency or OCR is raised.
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

dag_args = DEFAULT_ARGS.copy()
dag_args.update({
    'trigger_rule': TriggerRule.NONE_FAILED,
    'on_failure_callback': Slack.notify_task_failure})


@task.virtualenv(requirements=["pyhocon==0.3.61"], system_site_packages=True)
def resolve_input(resource_code: str, dataset_suffix: str,
                  output_text_path: str, output_tables_path: str) -> dict:
    """
    Resolve everything ``parse_and_write`` needs to read the curated input table and write its
    output paths

    Builds the curated source id ``curated_{resource_code}_{dataset_suffix}``, looks it up via
    :class:`DatalakeConfig`, and returns its primitive dict:
        {
            "input_uri":    <s3:// uri of the curated OBX table>,
            "minio_conn_id": <connection id chosen for that bucket>,
            "config":        <DatalakeConfig.to_dict() — the populated source/storage>,
            "text_uri":      <s3:// output uri for the parsed-report Delta table>,
            "tables_uri":    <s3:// output uri for the extracted-tables Delta table>,
        }

    Both output paths are **required**

    :param resource_code: Short resource code, e.g. "radimage" or "softpath".
    :param dataset_suffix: Completes the curated source id; defaults to "hl7_oru_r01_obx".
    :param output_text_path: Required s3:// uri for the parsed-report Delta output.
    :param output_tables_path: Required s3:// uri for the extracted-tables Delta output.
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
        "tables_uri": output_tables_path,
    }


@task.virtualenv(requirements=PARSE_REQUIREMENTS, system_site_packages=True,
                 executor_config=PARSE_EXECUTOR_CONFIG)
def parse_and_write(input_info: dict, start_date: str, end_date: str, row_limit: int,
                    doc_batch_concurrency: int, enable_ocr: bool) -> dict:
    """
    Read the curated OBX PDFs for the given date range, parse each with docling, and write two
    Delta outputs (keyed by ``hl7_id``): a parsed-report table (markdown) and an extracted-tables
    table (one row per table, CSV string).

    Helpers are nested (a fresh venv interpreter cannot see the DAG module's globals). Docling runs
    in-process with threaded multi-document concurrency (``settings.perf.doc_batch_concurrency``);
    one shared model copy keeps memory bounded on the single pod.

    NOTE (integration — could not be run offline; verify against the pinned lib versions):
      * docling API: ``DocumentConverter.convert_all`` / ``ConversionResult.{input.file, status,
        document}`` / ``TableItem.export_to_dataframe`` (signature varies — handled defensively);
      * polars/deltalake ``write_delta`` replaceWhere predicate + first-run table creation;
      * the MinIO endpoint key on the Airflow connection (``extra.endpoint_url``).

    :param input_info: Output of ``resolve_input`` (input_uri, minio_conn_id, text/tables uris).
    :param start_date: Inclusive start of the ``dte_of_message`` range (yyyy-MM-dd).
    :param end_date: Inclusive end of the ``dte_of_message`` range (yyyy-MM-dd).
    :param row_limit: Max PDFs processed this run (cap applied after the date filter).
    :param doc_batch_concurrency: docling threaded multi-document concurrency.
    :param enable_ocr: Run OCR (for scanned PDFs). Table-structure detection is always on.
    :return: Small dict of counts for logging/observability.
    """
    import base64
    import logging
    import tempfile

    import polars as pl
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

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

    # ---- S3 / delta-rs credentials ----
    def build_storage_options(conn_id: str) -> dict:
        conn = S3Hook(aws_conn_id=conn_id).get_connection(conn_id)
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

    # ---- input read: lazy scan + project + date filter + cap, single collect ----
    def read_obx_pdfs(storage_options: dict) -> pl.DataFrame:
        return (
            pl.scan_delta(input_info["input_uri"], storage_options=storage_options)
            .select(["hl7_id", "message_id", "set_id", "observation_value_base64", "dte_of_message"])
            .filter(pl.col("dte_of_message").is_between(pl.lit(start_date), pl.lit(end_date)))
            .limit(row_limit)
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

    # ---- docling: threaded multi-document conversion ----
    def build_converter():
        from docling.datamodel.base_models import InputFormat
        from docling.datamodel.pipeline_options import PdfPipelineOptions
        from docling.datamodel.settings import settings
        from docling.document_converter import DocumentConverter, PdfFormatOption

        settings.perf.doc_batch_concurrency = doc_batch_concurrency
        settings.perf.doc_batch_size = doc_batch_concurrency

        pipe_opts = PdfPipelineOptions()
        pipe_opts.do_table_structure = True   # required for Task 2 (table extraction)
        pipe_opts.do_ocr = enable_ocr
        return DocumentConverter(
            format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pipe_opts)})

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
        predicate = f"dte_of_message >= '{start_date}' AND dte_of_message <= '{end_date}'"
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
    logging.info("Read %d OBX rows for %s..%s (cap %d)", df.height, start_date, end_date, row_limit)

    with tempfile.TemporaryDirectory() as tmp_dir:
        pdf_files, meta_by_stem, skipped_rows = materialize_pdfs(df, tmp_dir)
        logging.info("Materialized %d PDFs (%d skipped non-PDF/decode)",
                     len(pdf_files), len(skipped_rows))
        results = list(build_converter().convert_all(pdf_files, raises_on_error=False)) \
            if pdf_files else []
        report_df, tables_df = build_outputs(results, meta_by_stem, skipped_rows)

    write_delta_overwrite(report_df, input_info["text_uri"], storage_options)
    write_delta_overwrite(tables_df, input_info["tables_uri"], storage_options)
    logging.info("Wrote %d report rows and %d table rows", report_df.height, tables_df.height)

    return {"rows_read": df.height, "pdfs_parsed": len(pdf_files),
            "skipped": len(skipped_rows), "tables_extracted": tables_df.height}


with DAG(
        dag_id="curated_hl7_parsing",
        params={
            "resource_code": Param("softpath", type="string",
                                   description="Short resource code. Ex: radimage | softpath"),
            "dataset_suffix": Param(DEFAULT_DATASET_SUFFIX, type="string",
                                    description="Completes the curated source id "
                                                "curated_<resource_code>_<dataset_suffix>."),
            "start_date": Param("", type="string",
                                description="Inclusive start of the dte_of_message range. Ex: 2025-08-15"),
            "end_date": Param("", type="string",
                              description="Inclusive end of the dte_of_message range. Ex: 2025-08-20"),
            "row_limit": Param(1000, type="integer",
                               description="Max number of PDFs processed per run (cap applied after the date filter)."),
            "doc_batch_concurrency": Param(4, type="integer",
                                           description="docling threaded multi-document concurrency (1 = sequential)."),
            "enable_ocr": Param(False, type="boolean",
                                description="Run OCR for scanned PDFs (slower). Table detection is always on."),
        },
        default_args=dag_args,
        doc_md=__doc__,
        start_date=datetime(2024, 12, 16),
        is_paused_upon_creation=True,
        render_template_as_native_obj=True,
        schedule_interval=None,
        tags=["curated", "hl7", "docling"],
        on_failure_callback=Slack.notify_dag_failure,
) as dag:
    resolved_input = resolve_input(
        resource_code="{{ params.resource_code }}",
        dataset_suffix="{{ params.dataset_suffix }}",
        # Hardcoded s3:// destinations (polars/deltalake use s3://, not s3a://), built inline so the
        # resource_code + dataset_suffix make each resource's output tables unique. Replace <bucket>/<key>.
        output_text_path="s3://yellow-prd/robertcaterev01/hl7_pipeline_1/{{ params.resource_code }}_{{ params.dataset_suffix }}_report_markdown",
        output_tables_path="s3://yellow-prd/robertcaterev01/hl7_pipeline_1/{{ params.resource_code }}_{{ params.dataset_suffix }}_extracted_tables",
    )

    parse_and_write(
        input_info=resolved_input,
        start_date="{{ params.start_date }}",
        end_date="{{ params.end_date }}",
        row_limit="{{ params.row_limit }}",
        doc_batch_concurrency="{{ params.doc_batch_concurrency }}",
        enable_ocr="{{ params.enable_ocr }}",
    )
