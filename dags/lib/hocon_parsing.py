def parse_hocon_conf(
        skip: bool = False
) -> dict:
    """
    Parse the HOCON configuration file for the Green zone,
    seeking the destination buckets for published ETLs

    :return: Parsed configuration as a dictionary.
    """

    # Imports made inside function due to using PythonVirtualenvOperator
    import logging
    import os
    import pyhocon as hocon

    from airflow.exceptions import AirflowFailException, AirflowSkipException
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    from lib.config import YELLOW_MINIO_CONN_ID, SPARK_BUCKET, CONFIG_FILE

    if skip:
        logging.info("Skipping HOCON parsing as requested.")
        raise AirflowSkipException()

    # local dir for keeping the file
    local_conf_directory = ("tmp/conf")
    os.makedirs(local_conf_directory, exist_ok=True)

    # Get S3 client
    s3 = S3Hook(aws_conn_id=YELLOW_MINIO_CONN_ID)
    s3_client = s3.get_conn()

    config_file_exists = s3.check_for_key(
        key=CONFIG_FILE,
        bucket_name=SPARK_BUCKET,
    )

    # Checking if the conf file exists
    if not config_file_exists:
        raise AirflowFailException(f"File {CONFIG_FILE} not found in bucket {SPARK_BUCKET}.")

    local_file_path = os.path.join(local_conf_directory, os.path.basename(CONFIG_FILE))

    # Downloading the file locally
    s3_client.download_file(SPARK_BUCKET, CONFIG_FILE, local_file_path)

    config = None
    logging.info("HOCON PARSING STARTED")
    config = hocon.ConfigFactory.parse_file(local_file_path)
    logging.info("HOCON PARSING COMPLETED")

    return config


def get_bucket_name(source_id: str, config: dict) -> str:
    """
    Get the bucket id for a given source id from the Spark HOCON configuration.

    :param source_id: The source id to look up.
    :param config: The parsed HOCON configuration.
    :return: The bucket id associated with the source id.
    """
    # Imports made inside function due to using PythonVirtualenvOperator
    from airflow.exceptions import AirflowFailException

    sources = config.get("datalake.sources")

    storageid = None

    for source in sources:
        if source["id"] == source_id:
            storageid = source["storageid"]

    if storageid is None:
        raise AirflowFailException(f"Source ID {source_id} not found in configuration.")

    storages = config.get("datalake.storages")

    for storage in storages:
        if storage["id"] == storageid:
            return storage["path"].split("/")[2]  # Extract bucket name from s3 path

    raise AirflowFailException(f"Storage ID {storageid} not found in configuration.")


def get_dataset_published_path(
        source_id: str,
        config: dict
) -> str:
    """
    Get the published path for a dataset from the Spark HOCON configuration.
    The path comes without the extension for the output file.

    :param source_id: The dataset ID to look up.
    :param config: The parsed HOCON configuration.
    :return: The published path associated with the dataset ID.
    """
    # Imports made inside function due to using PythonVirtualenvOperator
    from airflow.exceptions import AirflowFailException

    sources = config.get("datalake.sources")

    for source in sources:
        if source["id"] == source_id:
            return source["path"]

    raise AirflowFailException(f"Dataset ID {source_id} not found in configuration.")


def get_released_bucket_name(resource_code: str, config: dict) -> str:
    """
    Get the bucket name for a released resource code from the Spark HOCON configuration.

    :param resource_code: The resource code to look up.
    :param config: The parsed HOCON configuration.
    :return: The bucket name associated with the resource code.
    """
    from airflow.exceptions import AirflowFailException

    sources = config.get("datalake.sources")
    storages = config.get("datalake.storages")

    for source in sources:
        if f"released_{resource_code}" in source["id"]:
            storage_id = source["storageid"]

            for storage in storages:
                if storage["id"] == storage_id:
                    return storage["path"].split("/")[2]

    raise AirflowFailException(f"Resource code {resource_code} does not have any released configurations.")
