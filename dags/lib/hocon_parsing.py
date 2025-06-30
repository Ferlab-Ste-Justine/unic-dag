


def greenzone_hocon_parsing(
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

    from lib.config import MINIO_CONN_ID, SPARK_BUCKET, CONFIG_FILE
    from lib.exceptions import AirflowInputParsingException, MinioFileNotFoundException

    if skip:
        logging.info("Skipping HOCON parsing as requested.")
        raise AirflowSkipException()

    #local dir for keeping the file
    local_conf_directory = ("tmp/conf")
    os.makedirs(local_conf_directory, exist_ok=True)


    #Get S3 client
    s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
    s3_client = s3.get_conn()

    #Checking if the conf file exists
    if (not s3.check_for_key(
        key = CONFIG_FILE,
        bucket_name = SPARK_BUCKET
    )):
        raise MinioFileNotFoundException(bucket = SPARK_BUCKET, key = CONFIG_FILE)

    local_file_path = os.path.join(local_conf_directory, os.path.basename(CONFIG_FILE))


    #Downloading the file locally
    try :
        s3_client.download_file(SPARK_BUCKET, CONFIG_FILE, local_file_path)
    except Exception as e:
        raise AirflowFailException(f"Failed to download the file: {SPARK_BUCKET}/{CONFIG_FILE}") from e


    config = None
    try:
        logging.info("HOCON PARSING STARTED")
        config = hocon.ConfigFactory.parse_file(local_file_path)
        logging.info("HOCON PARSING COMPLETED")
    except Exception as e:
        raise AirflowInputParsingException(input_id = CONFIG_FILE) from e

    try:
        published_sources_bucket_map = {}

        sources: hocon.ConfigTree = config.get("datalake.sources")

        #return published_sources_bucket_map
    except Exception as e:
        raise AirflowFailException(f"Failed to extract published sources from configuration") from e

    return config


def get_bucket_id(source_id:str, config:dict) -> str:
    """
    Get the bucket id for a given source id from the Spark HOCON configuration.

    :param source_id: The source id to look up.
    :param config: The parsed HOCON configuration.
    :return: The bucket id associated with the source id.
    """
    # Imports made inside function due to using PythonVirtualenvOperator
    from airflow.exceptions import AirflowFailException

    try:
        sources = config.get("datalake.sources")
        for source in sources:
            if source["id"] == source_id:
                return source["storageid"]
        raise AirflowFailException(f"Source ID {source_id} not found in configuration.")
    except Exception as e:
        raise AirflowFailException(f"Failed to get bucket id for source {source_id}: {e}") from e

def get_published_sources_bucket_map(config: dict) -> dict:

    sources = config.get("datalake.sources")
    bucket_map = {}
    for source in sources:
        if source["id"].startswith("published_"):
            bucket_map[source["id"]] = source["storageid"]

    return bucket_map