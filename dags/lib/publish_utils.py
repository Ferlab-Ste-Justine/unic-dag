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
    logging.info(f"Configuration for {resource_code} (version {version_to_publish}):")
    logging.info(f"Input bucket: {mini_config['input_bucket']}")
    logging.info(f"Has clinical data: {mini_config['has_clinical']}")
    logging.info(f"Has nominative data: {mini_config['has_nominative']}")

    logging.info("Tables to be published:")
    for source_id, source_info in mini_config['sources'].items():
        logging.info(f"  - {source_info['table']}:")
        logging.info(f"    Source ID: {source_id}")
        logging.info(f"    Output bucket: {source_info['output_bucket']}")
        logging.info(f"    Output path: {source_info['output_path']}")
