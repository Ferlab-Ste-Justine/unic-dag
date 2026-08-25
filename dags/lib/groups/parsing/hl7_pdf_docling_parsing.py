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
  1. ``extract_config`` — resolve the input + two output datasets into a ``DatalakeConfig`` and validate the
     outputs are in the nominative zone.
  2. ``parse_and_write`` — read the run's ``dte_of_message`` interval window, parse with docling, write outputs.
"""
# pylint: disable=import-outside-toplevel, import-error, too-many-locals, too-many-statements, fixme

# parse_and_write's body needs its own timedelta and a module-level `timedelta` would shadow it.
import datetime as dt

from airflow.decorators import task, task_group
from kubernetes.client import models as k8s

from lib.config import INTERVAL_START_DAY, INTERVAL_END_DAY

# Dependencies installed into the parse_and_write venv at task runtime.
PARSE_REQUIREMENTS = [
    "docling==2.55.1",
    "polars==1.12.0",
    "deltalake==0.22.3",
    "pyarrow==25.0.0",
]

# parse_and_write runs docling in its own KubernetesExecutor pod.
# executor_config / pod_override is resolved at DAG-parse time and is NOT Jinja-templatable
PARSE_POD_MEMORY = "24Gi"
PARSE_POD_CPU = "8"
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

# docling has nothing that can break a blocked model call, set so the task dies instead of hanging.
PARSE_EXECUTION_TIMEOUT = dt.timedelta(hours=2)

# ============================================================================================
# TEMPORARY (UNIC-2068 diagnosis): To be removed after parse_and_write diagnosis..
# ============================================================================================

# Hosts docling's model loading depends on, plus controls. pypi/pythonhosted and github are the
# controls: pip and easyocr both demonstrably succeeded in the failing run, so they MUST come back
# OK. huggingface.co only serves metadata and a 302; the weight files come from a separate CDN host
# that is frequently allowlisted separately, this is to test whether cdn is allowed on our infra.
# us.aws.cdn.hf.co is where both repos below actually redirected on 2026-08-24; the probe logs the
# host it is really sent to ("via="), so trust that over this list.
DIAGNOSE_HOSTS = [
    "pypi.org",
    "files.pythonhosted.org",
    "github.com",
    "objects.githubusercontent.com",
    "huggingface.co",
    "us.aws.cdn.hf.co",
    "download.pytorch.org",
]

# The two HF repos docling 2.55.1 downloads, as (repo_id, revision), transcribed from the 2.55.1
# wheel -- the defaults move between releases, so do not infer them from another version:
#   layout       layout_model_specs.py DOCLING_LAYOUT_HERON + LayoutOptions.model_spec
#   tableformer  table_structure_model.py download_models(); note the tag, not a branch
# The path inside each repo is NOT hardcoded: the probe reads the file listing from the HF API and
# picks a .safetensors, so a renamed weight yields a bad-listing report instead of a phantom 404.
DIAGNOSE_HF_MODELS = [
    ("ds4sd/docling-layout-heron", "main"),
    ("ds4sd/docling-models", "v2.3.0"),
]


@task(task_id="diagnose_runtime", executor_config=PARSE_EXECUTOR_CONFIG,
      execution_timeout=dt.timedelta(minutes=20), retries=0)
def diagnose_runtime() -> dict:
    """ Log what ``parse_and_write``'s pod can actually see and reach.

    A task with stdlib-only imports -- no venv, no pip -- so it cannot fail for install
    reasons

    Never raises: a broken probe must not skip the run that carries the faulthandler evidence.
    ``retries=0`` deliberately overrides ``DEFAULT_ARGS``' 3 -- a failure here is the answer.

    :return: ``{host_or_url: outcome}``, also pushed to XCom so it is visible in the UI.
    """
    import json
    import logging
    import os
    import platform
    import shutil
    import socket
    import time
    import urllib.parse
    import urllib.request

    def _log_file(path: str) -> None:
        """Log one /sys or /proc file's contents on a single line, tolerating its absence."""
        try:
            with open(path, encoding="utf-8") as handle:
                logging.info("%s = %s", path, handle.read().strip().replace("\n", " | "))
        except OSError as exc:
            logging.info("%s unreadable: %s", path, exc)

    def _runtime_facts() -> None:
        # cpu.max is the CFS quota pod_override asked for, while os.cpu_count() reports the NODE's
        # core count. The gap between the two is what makes native thread pools oversubscribe and
        # spin against the quota, so log both side by side.
        logging.info("python=%s cpu_count=%s sched_affinity=%s", platform.python_version(),
                     os.cpu_count(), len(os.sched_getaffinity(0)))
        for path in ("/sys/fs/cgroup/cpu.max", "/sys/fs/cgroup/cpu.stat",
                     "/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory.current"):
            _log_file(path)
        for path in ("/tmp", os.path.expanduser("~")):
            usage = shutil.disk_usage(path)
            logging.info("disk %-10s total=%.1fGi free=%.1fGi", path,
                         usage.total / 2 ** 30, usage.free / 2 ** 30)
        # The model caches land under HOME; if it is unset or read-only, downloads fail oddly.
        logging.info("HOME=%s TMPDIR=%s home_writable=%s", os.environ.get("HOME"),
                     os.environ.get("TMPDIR"), os.access(os.path.expanduser("~"), os.W_OK))
        # workers.env is unset in the prod Helm values, so a proxy the cluster expects would be
        # absent here -- which on its own would produce exactly this hang.
        logging.info("proxy/hf env: %s", {key: value for key, value in os.environ.items()
                                          if "proxy" in key.lower()
                                          or key.startswith(("HF_", "HUGGINGFACE"))} or "(none)")

    def _probe_host(host: str) -> str:
        """Resolve, then open and immediately close a TCP connection to ``host:443``."""
        try:
            addrs = sorted({info[4][0] for info in socket.getaddrinfo(host, 443)})
        except OSError as exc:
            logging.warning("DNS  FAIL %-32s %s: %s", host, type(exc).__name__, exc)
            return f"dns-fail:{type(exc).__name__}"
        started = time.monotonic()
        try:
            with socket.create_connection((host, 443), timeout=10):
                pass  # a completed handshake is the whole test
            logging.info("TCP  OK   %-32s %5.2fs %s", host, time.monotonic() - started, addrs)
            return "ok"
        except OSError as exc:
            # An elapsed time near the full timeout means packets are being DROPPED, which is what
            # produces an unbounded block instead of an exception. A fast refusal is different
            logging.warning("TCP  FAIL %-32s %5.2fs %s: %s -> %s", host,
                            time.monotonic() - started, type(exc).__name__, exc, addrs)
            return f"tcp-fail:{type(exc).__name__}"

    def _get(url: str, headers: dict, timeout: int):
        """GET ``url`` following redirects; return (body, final_url)."""
        request = urllib.request.Request(
            url, headers={"User-Agent": "unic-dag-egress-probe", **headers})
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.read(), response.geturl()

    def _probe_hf_model(repo_id: str, revision: str) -> str:
        """List a HF repo, then range-GET one of its weight blobs and name the CDN host.

        Two hops on purpose, because they are two different hosts and either can be the one that
        is blocked: the listing is served by huggingface.co, the blob by a CDN it 302s to.
        """
        api = f"https://huggingface.co/api/models/{repo_id}/revision/{revision}"
        started = time.monotonic()
        try:
            body, _ = _get(api, {}, 20)
            files = [f["rfilename"] for f in json.loads(body).get("siblings", [])]
            logging.info("HF   API  OK   %-34s rev=%-8s %5.2fs files=%s",
                         repo_id, revision, time.monotonic() - started, files)
        except Exception as exc:  # pylint: disable=broad-except
            logging.warning("HF   API  FAIL %-34s rev=%-8s %5.2fs %s: %s", repo_id, revision,
                            time.monotonic() - started, type(exc).__name__, exc)
            return f"api-fail:{type(exc).__name__}"

        weights = [f for f in files if f.endswith(".safetensors")]
        if not weights:
            logging.warning("HF   no .safetensors in %s@%s; listing was %s", repo_id, revision, files)
            return "no-weights-in-listing"

        url = f"https://huggingface.co/{repo_id}/resolve/{revision}/{weights[0]}"
        started = time.monotonic()
        try:
            body, final_url = _get(url, {"Range": "bytes=0-1023"}, 30)
            host = urllib.parse.urlparse(final_url).hostname
            logging.info("HF   BLOB OK   %-34s %s bytes=%d via=%s %5.2fs",
                         repo_id, weights[0], len(body), host, time.monotonic() - started)
            return f"ok via {host}"
        except Exception as exc:  # pylint: disable=broad-except
            logging.warning("HF   BLOB FAIL %-34s %s %5.2fs %s: %s", repo_id, weights[0],
                            time.monotonic() - started, type(exc).__name__, exc)
            return f"blob-fail:{type(exc).__name__}"

    logging.info("=== runtime facts ===")
    _runtime_facts()
    logging.info("=== dns, then tcp:443 ===")
    summary = {host: _probe_host(host) for host in DIAGNOSE_HOSTS}
    logging.info("=== huggingface: list repo, then range-GET a weights blob ===")
    summary.update({repo: _probe_hf_model(repo, rev) for repo, rev in DIAGNOSE_HF_MODELS})
    logging.info("=== summary ===")
    for key, value in summary.items():
        logging.info("%-100s %s", key, value)
    return summary

# ==================== end of the TEMPORARY UNIC-2068 diagnosis block ========================


@task.virtualenv(requirements=["pyhocon==0.3.61"], system_site_packages=True)
def extract_config(input_source_id: str, report_delta_destination_id: str,
                   tables_destination_id: str, report_md_destination_id: str) -> dict:
    """
    Load ``config/prod.conf``, resolve the input + three outputs, and validate that the output paths
    are nominative.

    :param input_source_id: datalake.sources id of the curated OBX Delta input table.
    :param report_delta_destination_id: datalake.sources id of the parsed-report Delta output.
    :param tables_destination_id: datalake.sources id of the extracted-tables CSV-tree output pattern.
    :param report_md_destination_id: datalake.sources id of the per-document report.md tree output pattern.
    :return: ``DatalakeConfig.to_dict()``
    """
    from airflow.exceptions import AirflowFailException

    from lib.config import NOMINATIVE_BUCKET
    from lib.datalake_config import DatalakeConfig

    output_ids = (report_delta_destination_id, tables_destination_id, report_md_destination_id)
    config = DatalakeConfig(sources_id_list={input_source_id, *output_ids})

    # The parsed HL7 outputs hold nominative data.
    for dataset_id in output_ids:
        bucket = config.bucket_for_source(dataset_id)
        if bucket != NOMINATIVE_BUCKET:
            raise AirflowFailException(
                f"Output dataset '{dataset_id}' resolves to bucket '{bucket}', "
                f"expected the nominative bucket '{NOMINATIVE_BUCKET}'.")

    return config.to_dict()


@task.virtualenv(requirements=PARSE_REQUIREMENTS, system_site_packages=True,
                 executor_config=PARSE_EXECUTOR_CONFIG,
                 execution_timeout=PARSE_EXECUTION_TIMEOUT,
                 retries=1)
def parse_and_write(config_dict: dict, input_source_id: str, report_delta_destination_id: str,
                    tables_destination_id: str, report_md_destination_id: str,
                    interval_start: str, interval_end: str,
                    doc_batch_concurrency: int, enable_ocr: bool) -> dict:
    """
    Read the curated OBX PDFs for the given date range, parse each with docling, and write three
    outputs (keyed by ``hl7_id``): a parsed-report Delta table (markdown), the extracted tables as a
    date-first CSV tree (one CSV per table), and a ``report.md`` per document in that same tree — via
    ``lib.hl7_io_utils.{write_report_delta, write_tables, write_report_markdown_tree}``. Documents are
    processed one ``dte_of_message`` partition at a time (each overwriting its own partition), bounding
    peak memory to a single day.

    :param config_dict: ``DatalakeConfig.to_dict()``
    :param input_source_id: datalake.sources id of the curated OBX Delta input table.
    :param report_delta_destination_id: datalake.sources id of the parsed-report Delta output.
    :param tables_destination_id: datalake.sources id of the extracted-tables CSV-tree output pattern.
    :param report_md_destination_id: datalake.sources id of the per-document report.md tree output pattern.
    :param interval_start: Inclusive start of the run's ``dte_of_message`` window (yyyy-MM-dd).
    :param interval_end: Exclusive end of the run's ``dte_of_message`` window (yyyy-MM-dd).
    :param doc_batch_concurrency: docling threaded multi-document concurrency.
    :param enable_ocr: Run OCR (for scanned PDFs). Table-structure detection is always on.
    :return: Per-window counts (``rows_read``, ``pdfs_parsed``, ``skipped``, ``tables_extracted``,
        ``reports_written``) plus ``dates`` — the number of ``dte_of_message`` partitions processed.
    """
    # --- TEMPORARY (UNIC-2068 diagnosis): ordering-sensitive, keep first --------------------
    # os env vars MUST precede every other import in this body: huggingface_hub
    # freezes its timeouts into module constants at import time, and OpenMP reads OMP_NUM_THREADS
    # when its runtime initializes at `import torch`. Setting them later has no effect.
    import os

    # Turn a stalled HuggingFace transfer into an exception instead of an unbounded block.
    os.environ.setdefault("HF_HUB_ETAG_TIMEOUT", "20")
    os.environ.setdefault("HF_HUB_DOWNLOAD_TIMEOUT", "60")
    # Match the pod's CFS quota, or the BLAS/OpenMP pools size themselves off the node's core
    # count and thrash. Literal because module globals (PARSE_POD_CPU) are NOT visible inside the
    # venv subprocess -- keep the two in sync.
    os.environ.setdefault("OMP_NUM_THREADS", "8")
    # --- end of the ordering-sensitive part -------------------------------------------------

    import base64
    import faulthandler
    import logging
    import tempfile
    import threading
    import time
    from datetime import date, timedelta
    from pathlib import Path

    import polars as pl
    from airflow.exceptions import AirflowFailException
    from typing import Tuple

    from lib.datalake_config import DatalakeConfig
    from lib.docling_utils import build_converter, run
    from lib.hl7_io_utils import (_detect_format, build_storage_options, delete_report_tree_for_date,
                                   write_report_delta, write_report_markdown_tree, write_tables)

    # --- TEMPORARY (UNIC-2068 diagnosis): make a silent block describe itself ---------------
    # execution_timeout fires SIGALRM in the PARENT airflow process running
    # execute_in_subprocess, so its traceback points at proc.stdout.readline(), never at docling.
    # faulthandler runs in THIS process and is the only thing that names the real blocking frame.
    faulthandler.dump_traceback_later(300, repeat=True, exit=False)

    # docling mutes its own model download (download_hf_model calls disable_progress_bars before
    # snapshot_download), which is why the log simply stops. Make huggingface_hub and urllib3
    # narrate every HTTP request instead: one line per request with the URL, which names the CDN
    # host and shows exactly which call never comes back.
    for logger_name in ("huggingface_hub", "urllib3.connectionpool", "filelock"):
        logging.getLogger(logger_name).setLevel(logging.DEBUG)

    def _heartbeat() -> None:
        """Log liveness plus whether the HuggingFace cache is still growing, every 30s.

        The in-dag replacement for ``find ~/.cache/huggingface -name '*.incomplete'``, which
        needs a shell in the pod: a cache whose byte count keeps rising is a slow download, one
        that is static is a stalled one. Because this runs on its own thread, whether it *keeps
        logging* is itself a measurement -- if it goes quiet too, the GIL is held by native code
        that never releases it, i.e. the task is compute-bound rather than blocked on the network.
        """
        started = time.monotonic()
        while True:
            time.sleep(30)
            cache_bytes = 0
            for root, _dirs, files in os.walk(os.path.expanduser("~/.cache/huggingface")):
                for name in files:
                    try:
                        cache_bytes += os.path.getsize(os.path.join(root, name))
                    except OSError:
                        pass  # a partial file can vanish mid-walk; its bytes are not the point
            try:
                with open("/proc/self/status", encoding="utf-8") as handle:
                    status = dict(line.split(":", 1) for line in handle if ":" in line)
                rss = status.get("VmRSS", "?").strip()
                threads = status.get("Threads", "?").strip()
            except OSError:
                rss, threads = "?", "?"
            logging.info("HEARTBEAT t+%ds rss=%s threads=%s hf_cache=%.1fMiB",
                         int(time.monotonic() - started), rss, threads, cache_bytes / 2 ** 20)

    threading.Thread(target=_heartbeat, daemon=True, name="unic-heartbeat").start()
    # --- end TEMPORARY ----------------------------------------------------------------------

    config = DatalakeConfig.from_dict(config_dict)

    # The half-open window [interval_start, interval_end) must span at least one day.
    if interval_start >= interval_end:
        raise AirflowFailException(
            f"Empty dte_of_message window [{interval_start}, {interval_end}), must be daily or coarser."
        )

    storage_options = build_storage_options(config.minio_conn_id)
    report_delta_uri = config.source_s3_path(report_delta_destination_id, scheme="s3")
    tables_pattern_uri = config.source_s3_path(tables_destination_id, scheme="s3")
    report_md_pattern_uri = config.source_s3_path(report_md_destination_id, scheme="s3")

    # Explicit column types for the two output frames declared so that:
    # (1) an empty result still yields a correctly-columned frame;
    # (2) all-null columns (parse_error on success, page_no when docling
    # gives no page) keep their type instead of becoming a Null column;
    # (3) every run produces an identical schema, so the Delta table stays schema-stable across overwrites.
    report_schema = {
        "hl7_id": pl.Utf8, "dte_of_message": pl.Utf8,
        "report_markdown": pl.Utf8, "source_format": pl.Utf8,
        "parse_status": pl.Utf8, "parse_error": pl.Utf8,
    }
    tables_schema = {
        "hl7_id": pl.Utf8, "dte_of_message": pl.Utf8,
        "table_index": pl.Int64, "table_csv": pl.Utf8,
        "n_rows": pl.Int64, "n_cols": pl.Int64, "page_no": pl.Int64,
    }

    # ---- input read: per-date, partition-pruned ----
    def _partition_dates_in_window() -> list[str]:
        # Distinct dte_of_message partitions overlapping [interval_start, interval_end), read from the
        # Delta metadata (no data files scanned). dte_of_message is a yyyy-MM-dd string, so the bound
        # comparison is chronological.
        from deltalake import DeltaTable

        table = DeltaTable(config.source_s3_path(input_source_id, scheme="s3"),
                           storage_options=storage_options)
        dates = {part["dte_of_message"] for part in table.partitions()}
        return sorted(d for d in dates if interval_start <= d < interval_end)

    def _read_obx_pdfs_for_date(d: str) -> pl.DataFrame:
        return (
            pl.scan_delta(config.source_s3_path(input_source_id, scheme="s3"),
                          storage_options=storage_options)
            .select(["hl7_id", "observation_value_base64", "dte_of_message"])
            .filter(pl.col("dte_of_message") == d)  # equality on the partition column prunes to one date
            .collect()
        )

    # ---- decode base64 -> temp files; detect format; skip non-PDF ----
    def _materialize_pdfs(df: pl.DataFrame, tmp_dir) -> tuple[list[Path], dict[str, dict], list[dict]]:
        pdf_files, meta_by_stem, skipped_rows = [], {}, []
        for i, row in enumerate(df.iter_rows(named=True)):
            keys = {"hl7_id": row["hl7_id"], "dte_of_message": row["dte_of_message"]}
            try:
                raw = base64.b64decode(row["observation_value_base64"])
            except Exception as exc:  # pylint: disable=broad-except
                skipped_rows.append({**keys, "report_markdown": None, "source_format": "other",
                                     "parse_status": "skipped", "parse_error": f"base64 decode: {exc}"})
                continue
            fmt = _detect_format(raw)
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

    def _table_to_csv(table, document) -> Tuple[str, int, int, int]:
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
    def _build_outputs(results, meta_by_stem, skipped_rows):
        report_rows = list(skipped_rows)
        table_rows = []
        for result in results:
            stem = result.input.file.stem
            keys = meta_by_stem.get(stem, {"hl7_id": stem, "dte_of_message": None})
            status = getattr(result.status, "name", str(result.status)).lower()
            if status not in ("success", "partial_success"):
                report_rows.append({**keys, "report_markdown": None, "source_format": "pdf",
                                    "parse_status": status, "parse_error": "docling conversion failed"})
                continue
            document = result.document
            report_rows.append({**keys, "report_markdown": document.export_to_markdown(),
                                "source_format": "pdf", "parse_status": status, "parse_error": None})
            for idx, table in enumerate(document.tables):
                csv, n_rows, n_cols, page_no = _table_to_csv(table, document)
                table_rows.append({**keys, "table_index": idx, "table_csv": csv,
                                   "n_rows": n_rows, "n_cols": n_cols, "page_no": page_no})
        return (pl.DataFrame(report_rows, schema=report_schema),
                pl.DataFrame(table_rows, schema=tables_schema))

    # ---- process one dte_of_message partition end-to-end ----
    def _process_date(d: str, converter, tmp_dir: str) -> dict:
        """Read, parse, and overwrite a single ``dte_of_message`` partition; return its counts.

        ``tmp_dir`` is shared across dates: each date's PDFs are materialized, parsed, and written before
        the next date runs, and ``run`` only ever receives this date's ``pdf_files`` — so per-date stems
        may overwrite an earlier date's files harmlessly.
        """
        next_d = (date.fromisoformat(d) + timedelta(days=1)).isoformat()  # exclusive upper of the 1-day window
        df_d = _read_obx_pdfs_for_date(d)
        pdf_files, meta_by_stem, skipped_rows = _materialize_pdfs(df_d, tmp_dir)
        results = run(converter, pdf_files) if pdf_files else []
        report_df, tables_df = _build_outputs(results, meta_by_stem, skipped_rows)

        # Overwrite this date only: clear the shared day-folder, then replace the report Delta partition.
        delete_report_tree_for_date(tables_pattern_uri, d, config.minio_conn_id)
        write_report_delta(report_df, report_uri=report_delta_uri, storage_options=storage_options,
                           window_start=d, window_end=next_d)
        write_tables(tables_df, tables_pattern_uri=tables_pattern_uri, minio_conn_id=config.minio_conn_id)
        reports_written = write_report_markdown_tree(
            report_df, report_md_pattern_uri=report_md_pattern_uri, minio_conn_id=config.minio_conn_id)

        logging.info("[%s] %d rows, %d PDFs, %d skipped, %d tables, %d report.md",
                     d, df_d.height, len(pdf_files), len(skipped_rows), tables_df.height, reports_written)
        return {"rows_read": df_d.height, "pdfs_parsed": len(pdf_files), "skipped": len(skipped_rows),
                "tables_extracted": tables_df.height, "reports_written": reports_written}

    # ---- orchestration: process each date partition, accumulate counts ----
    # --- TEMPORARY (UNIC-2068 diagnosis) ----------------------------------------------------
    # Name the model docling is about to fetch BEFORE the call that hangs. Also yields the exact
    # repo_id and revision for this docling version, which must be obtained since docling's default
    # layout spec changes between minor versions.
    try:
        from docling.datamodel.pipeline_options import LayoutOptions
        from docling.datamodel.settings import settings as docling_settings
        model_spec = LayoutOptions().model_spec
        logging.info("docling layout model: repo_id=%s revision=%s cache_dir=%s artifacts_path=%s",
                     model_spec.repo_id, getattr(model_spec, "revision", None),
                     docling_settings.cache_dir, docling_settings.artifacts_path)
    except Exception as exc:  # pylint: disable=broad-except
        logging.warning("could not resolve docling layout model spec: %s", exc)
    # --- end TEMPORARY ----------------------------------------------------------------------

    converter = build_converter(doc_batch_concurrency, enable_ocr)
    dates = _partition_dates_in_window()
    logging.info("Processing %d date partition(s) in [%s, %s)", len(dates), interval_start, interval_end)

    logging_statistics = {"rows_read": 0, "pdfs_parsed": 0, "skipped": 0,
                          "tables_extracted": 0, "reports_written": 0}
    with tempfile.TemporaryDirectory() as tmp_dir:  # one temp dir shared by every date in this run
        for d in dates:
            for key, count in _process_date(d, converter, tmp_dir).items():
                logging_statistics[key] += count

    return {**logging_statistics, "dates": len(dates)}


@task_group(group_id="hl7_pdf_docling_parsing")
def hl7_pdf_docling_parsing(input_source_id: str, report_delta_destination_id: str,
                            tables_destination_id: str, report_md_destination_id: str,
                            doc_batch_concurrency: int = 4, enable_ocr: bool = False) -> None:
    """Resolve the curated OBX table, then parse its PDFs and write report + tables.

    The date window is each run's own ``data_interval`` (half-open ``[start, end)``)

    :param input_source_id: datalake.sources id of the curated OBX Delta input table.
    :param report_delta_destination_id: datalake.sources id of the parsed-report Delta output dataset.
    :param tables_destination_id: datalake.sources id of the extracted-tables CSV-tree output pattern.
    :param report_md_destination_id: datalake.sources id of the per-document report.md tree output pattern.
    :param doc_batch_concurrency: docling threaded multi-document concurrencdatasety (1 = sequential).
    :param enable_ocr: Run OCR for scanned PDFs (table-structure detection is always on).
    """
    # TEMPORARY (UNIC-2068 diagnosis)
    diagnostics = diagnose_runtime()

    config_dict = extract_config(
        input_source_id=input_source_id,
        report_delta_destination_id=report_delta_destination_id,
        tables_destination_id=tables_destination_id,
        report_md_destination_id=report_md_destination_id,
    )

    diagnostics >> config_dict  # TEMPORARY (UNIC-2068 diagnosis)

    parse_and_write(
        config_dict=config_dict,
        input_source_id=input_source_id,
        report_delta_destination_id=report_delta_destination_id,
        tables_destination_id=tables_destination_id,
        report_md_destination_id=report_md_destination_id,
        interval_start=INTERVAL_START_DAY,
        interval_end=INTERVAL_END_DAY,
        doc_batch_concurrency=doc_batch_concurrency,
        enable_ocr=enable_ocr,
    )
