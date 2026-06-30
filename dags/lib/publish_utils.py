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
                                        input_bucket: str = None,
                                        output_bucket: str = None) -> str:
    """
    Choose the Minio connection ID based on the provided mini-config or use the provided ID.
    You can either specify only an input bucket, or both.


    :param minio_conn_id: The default Minio connection ID to use if no input bucket is specified.
    :param input_bucket: Input bucket from the resource configuration extracted by "extract_config_info".
    :param output_bucket: Output bucket from the resource configuration extracted by "extract_config_info".
    """
    from lib.config import GREEN_MINIO_CONN_ID, YELLOW_MINIO_CONN_ID, RED_MINIO_CONN_ID, RELEASED_BUCKET, \
        CATALOG_BUCKET, NOMINATIVE_BUCKET

    if output_bucket is None:
        return {
            RELEASED_BUCKET: GREEN_MINIO_CONN_ID,
            CATALOG_BUCKET: YELLOW_MINIO_CONN_ID,
            NOMINATIVE_BUCKET: RED_MINIO_CONN_ID,
        }.get(input_bucket, minio_conn_id)

    if "clinical" in output_bucket and (input_bucket is None or input_bucket == RELEASED_BUCKET):
        return GREEN_MINIO_CONN_ID
    if "clinical" in output_bucket and input_bucket == CATALOG_BUCKET:
        return YELLOW_MINIO_CONN_ID
    if "nominative" in output_bucket:
        return RED_MINIO_CONN_ID
    # If the output bucket does not match any known "released" buckets, return the provided Minio connection ID.
    return minio_conn_id
