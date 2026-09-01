"""
============================================================
                      docling_utils
============================================================

docling orchestration helpers for the HL7 PDF-parsing pipeline: constructing the
``DocumentConverter`` and running the conversion.

"""
from pathlib import Path


# pylint: disable=import-outside-toplevel, import-error


def build_converter(doc_batch_concurrency: int, enable_ocr: bool):
    """Build a docling ``DocumentConverter`` (table-structure on, OCR optional) from local weights.

    :param doc_batch_concurrency: Not true concurrency, only sets document batch size, ``settings.perf.doc_batch_size``
    :param enable_ocr: run OCR for scanned PDFs. Table-structure detection is always on.
    :raises AirflowFailException: the artifacts directory is missing -- fatal because we want to
        avoid downloading the model weights on each dag run.
    """
    import logging
    import os

    from airflow.exceptions import AirflowFailException

    resolved_path = os.environ.get("DOCLING_ARTIFACTS_PATH")
    if not resolved_path or not os.path.isdir(resolved_path):
        raise AirflowFailException(
            f"Error accessing Docling model weights / artifacts path : {resolved_path!r} is not a directory; this task must run"
            "on the docling image, which bakes the weights in and sets DOCLING_ARTIFACTS_PATH directory.")

    from docling.datamodel.accelerator_options import AcceleratorDevice, AcceleratorOptions
    from docling.datamodel.base_models import InputFormat
    from docling.datamodel.pipeline_options import PdfPipelineOptions
    from docling.datamodel.settings import settings
    from docling.document_converter import DocumentConverter, PdfFormatOption

    # Attempts to run parallel threads processing documents. Note: No benefit without free-threaded python 3.13-3.14
    # settings.perf.doc_batch_concurrency = doc_batch_concurrency

    settings.perf.doc_batch_size = doc_batch_concurrency

    pipe_opts = PdfPipelineOptions()
    pipe_opts.do_table_structure = True  # required for Task 2 (table extraction)
    pipe_opts.do_ocr = enable_ocr
    pipe_opts.artifacts_path = resolved_path
    # Pin to CPU rather than letting device="auto" probe. num_threads is left unset on purpose:
    # AcceleratorOptions fills it from $OMP_NUM_THREADS and defaults to 4 otherwise.
    pipe_opts.accelerator_options = AcceleratorOptions(device=AcceleratorDevice.CPU.value)

    converter = DocumentConverter(
        format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pipe_opts)})
    logging.info("[DOCLING] SUCCESS: Converter built from %s", resolved_path)
    return converter


def run(converter, pdf_files: list[Path]) -> list:
    """Convert ``pdf_files`` with an already-built ``converter`` and return the ConversionResults.

    Failures do not raise: a document that fails to convert comes back as a result carrying a
    failure status, so one malformed PDF does not abort the rest.

    :param converter: a converter from :func:`build_converter`.
    :param pdf_files: the input document paths.
    """
    return list(converter.convert_all(pdf_files, raises_on_error=False))
