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
    logging.info(f"+ Configuration for {resource_code} (version {version_to_publish})")
    logging.info(f"+ Input bucket: {mini_config['input_bucket']}")

    logging.info(f"+ Clinical bucket  : {mini_config['clinical_bucket']}")
    logging.info(f"+ Nominative bucket: {mini_config['nominative_bucket']}")

    # Extracted tables
    logging.info("Extracted Tables".center(50, "-"))
    for source_id, source_info in mini_config['sources'].items():
        logging.info(f"  - {source_info['table']}:")
        logging.info(f"    Source ID: {source_id}")
        logging.info(f"    Output bucket: {source_info['output_bucket']}")
        logging.info(f"    Output path: {source_info['output_path']}")
    logging.info("-" * 50)

    logging.info("=" * 50)


def choose_minio_conn_id(config, minio_conn_id) -> str:
    """
    Choose the Minio connection ID based on the provided input bucket from the
    mini-config or use the provided ID.

    :param config: The configuration dictionary containing the input bucket from "extract_config_info".
    :param minio_conn_id: The default Minio connection ID to use if no input bucket is specified.
    """
    from lib.config import GREEN_MINIO_CONN_ID, YELLOW_MINIO_CONN_ID, RED_MINIO_CONN_ID, RELEASED_BUCKET, \
        CATALOG_BUCKET, NOMINATIVE_BUCKET

    if config is None:
        return minio_conn_id
    else:
        input_bucket = config["input_bucket"]

        if input_bucket == RELEASED_BUCKET:
            return GREEN_MINIO_CONN_ID
        elif input_bucket == CATALOG_BUCKET:
            return YELLOW_MINIO_CONN_ID
        elif input_bucket == NOMINATIVE_BUCKET:
            return RED_MINIO_CONN_ID
        else:
            # If the input bucket does not match any known "released" buckets, return the provided Minio connection ID.
            return minio_conn_id
