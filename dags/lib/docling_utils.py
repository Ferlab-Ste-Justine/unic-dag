"""
============================================================
                      docling_utils
============================================================

docling orchestration helpers for the HL7 PDF-parsing pipeline: constructing the
``DocumentConverter`` and running the conversion.

All imports are deferred into the function bodies, the latter of which are intended
to be called from inside a `@task.virtualenv` with the appropriate dependencies.
"""


def build_converter(doc_batch_concurrency: int, enable_ocr: bool):
    """Build a docling ``DocumentConverter`` (table-structure on, OCR optional)."""
    from docling.datamodel.base_models import InputFormat
    from docling.datamodel.pipeline_options import PdfPipelineOptions
    from docling.datamodel.settings import settings
    from docling.document_converter import DocumentConverter, PdfFormatOption

    # Attempts to run parallel threads processing documents. Note: No benefit without free-threaded python 3.13-3.14
    # settings.perf.doc_batch_concurrency = doc_batch_concurrency

    settings.perf.doc_batch_size = doc_batch_concurrency

    pipe_opts = PdfPipelineOptions()
    pipe_opts.do_table_structure = True   # required for Task 2 (table extraction)
    pipe_opts.do_ocr = enable_ocr
    return DocumentConverter(
        format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pipe_opts)})


def run(converter, pdf_files) -> list:
    """Convert ``pdf_files`` with an already-built ``converter`` and return the ConversionResults.

    Takes the converter + the input document paths and calls ``convert_all``. Input is file paths
    for now; a future task may switch to in-memory ``DocumentStream``s.
    """
    return list(converter.convert_all(pdf_files, raises_on_error=False))
