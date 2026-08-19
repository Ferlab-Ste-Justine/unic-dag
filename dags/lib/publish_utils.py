# pylint: disable=import-outside-toplevel
import logging
from enum import Enum


class FileType(Enum):
    """
    Enum for output types.
    """
    EXCEL = ".xlsx"
    PARQUET = ".parquet"


def add_extension_to_path(path: str, output_type: FileType) -> str:
    """
    Append the extension to the path based on the output type.

    :param path: The base path to which the extension will be added, must NOT end with any extension.
    If the given base path has an extension, the new extension will be appended to it.
    :param output_type: The type of output, which determines the extension to append.
    """
    return f"{path}{output_type.value}"


def print_extracted_config(resource_code: str, version_to_publish: str, mini_config: dict) -> None:
    logging.info((f" Extracted {resource_code} configuration").center(50, "="))
    logging.info('+ Configuration for %s (version %s)', resource_code, version_to_publish)
    logging.info('+ Input bucket: %s', mini_config['input_bucket'])

    logging.info('+ Clinical bucket  : %s', mini_config['clinical_bucket'])
    logging.info('+ Nominative bucket: %s', mini_config['nominative_bucket'])

    # Extracted tables
    logging.info("Extracted Tables".center(50, "-"))
    for source_id, source_info in mini_config['sources'].items():
        logging.info('  - %s:', source_info['table'])
        logging.info('    Source ID: %s', source_id)
        logging.info('    Output bucket: %s', source_info['output_bucket'])
        logging.info('    Output path: %s', source_info['output_path'])
    logging.info("-" * 50)

    logging.info("=" * 50)


def determine_minio_conn_id_from_config(minio_conn_id: str,
                                        *input_buckets: str,
                                        output_bucket: str = None) -> str:
    """
    Choose the MinIO connection id granting the widest access across all the given buckets.

    :param minio_conn_id: Default connection id, used only when no bucket resolves to a zone.
    :param input_buckets: Zero or more source buckets to consider.
    :param output_bucket: Optional destination bucket (publish flow), considered alongside the inputs.
    """
    from lib.config import GREEN_MINIO_CONN_ID, YELLOW_MINIO_CONN_ID, RED_MINIO_CONN_ID, RELEASED_BUCKET, \
        CATALOG_BUCKET, NOMINATIVE_BUCKET, VNA_CLINIQUE_RED_BUCKET, VNA_CLINIQUE_YELLOW_BUCKET

    buckets = [bucket for bucket in (*input_buckets, output_bucket) if bucket]

    # The published buckets are matched on the zone pattern contained in their name.
    if any(bucket in (NOMINATIVE_BUCKET, VNA_CLINIQUE_RED_BUCKET) or "nominative" in bucket
           for bucket in buckets):
        return RED_MINIO_CONN_ID
    if any(bucket in (CATALOG_BUCKET, VNA_CLINIQUE_YELLOW_BUCKET) for bucket in buckets):
        return YELLOW_MINIO_CONN_ID
    if any(bucket == RELEASED_BUCKET or "clinical" in bucket for bucket in buckets):
        return GREEN_MINIO_CONN_ID
    return minio_conn_id
