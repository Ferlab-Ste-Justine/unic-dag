from __future__ import annotations

# pylint: disable=import-outside-toplevel, too-many-locals
from dataclasses import dataclass

# Frozen dataclasses are immutable and hashable, thus set-safe.
@dataclass(frozen=True)
class StorageConf:
    """A ``datalake.storages`` entry"""

    storage_id: str
    path: str  # the ENTIRE base uri, e.g. "s3a://red-prd" OR "s3a://red-prd/curated/x"

    @property
    def bucket(self) -> str:
        """Bucket name only"""
        return self.path.split("/")[2]

    def base_uri(self, scheme: str = "s3a") -> str:
        """Full storage address uri , re-schemed to ``scheme`` (e.g. s3a -> s3)."""
        _, _, remainder = self.path.partition("://")
        return f"{scheme}://{remainder.rstrip('/')}"

    def to_dict(self) -> dict:
        """Primitive form for XCom serialization."""
        return {"storage_id": self.storage_id, "path": self.path}

    @classmethod
    def from_dict(cls, data: dict) -> StorageConf:
        """Rebuild from `to_dict` output."""
        return cls(storage_id=data["storage_id"], path=data["path"])


@dataclass(frozen=True)
class SourceConf:
    """A ``datalake.sources`` entry"""

    source_id: str
    storage_id: str
    relative_path: str

    def source_s3_path(self, storage: StorageConf, scheme: str = "s3a") -> str:
        """Reconstruct the full dataset URI from the two values: the storage base uri (all
        levels) and this source's relative path."""
        return f"{storage.base_uri(scheme)}/{self.relative_path.lstrip('/')}"

    def to_dict(self) -> dict:
        """Primitive form for XCom serialization."""
        return {
            "source_id": self.source_id,
            "storage_id": self.storage_id,
            "relative_path": self.relative_path,
        }

    @classmethod
    def from_dict(cls, data: dict) -> SourceConf:
        """Rebuild from `to_dict` output."""
        return cls(
            source_id=data["source_id"],
            storage_id=data["storage_id"],
            relative_path=data["relative_path"],
        )


class DatalakeConfig:
    """
    ============================================================
                        DatalakeConfig
    ------------------------------------------------------------
        Structured typed access to the UnIC datalake HOCON configuration (``config/prod.conf``),
        exposed as two arrays, ``datalake.sources`` and ``datalake.storages``.
        Intended for use within a single Airflow task, but has a ``to_dict`` /
        ``from_dict`` pair for XCom serialization between airflow tasks.

        Imports of ``lib.*`` and Airflow are made inside methods because this module is imported inside
        ``@task.virtualenv`` task bodies.

        ``__init__`` loads + parses the config and, when ``sources_id_list`` is provided, populates
        :attr:`sources` / :attr:`storages`.
    ============================================================
    """

    def __init__(self, minio_conn_id: str = None, sources_id_list: set[str] = None):
        from lib.publish_utils import determine_minio_conn_id_from_config

        self.sources: list[SourceConf] = []
        self.storages: list[StorageConf] = []
        self.raw_config = self.load()

        if sources_id_list is not None:
            self.extract_config_info(sources_id_list=sources_id_list)

        # flatmap each populated source to its backing bucket, then let the function fold the
        # buckets down to the widest-access connection id (red > yellow > green).
        source_buckets = [self.bucket_for_source(source.source_id) for source in self.sources]
        self.minio_conn_id = determine_minio_conn_id_from_config(minio_conn_id, *source_buckets)

    @staticmethod
    def load() -> dict:
        """Download ``config/prod.conf`` from the spark bucket and parse it with pyhocon."""
        import logging
        import os
        import pyhocon as hocon

        from airflow.exceptions import AirflowFailException
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        from lib.config import YELLOW_MINIO_CONN_ID, SPARK_BUCKET, CONFIG_FILE

        local_conf_directory = "tmp/conf"
        os.makedirs(local_conf_directory, exist_ok=True)

        s3 = S3Hook(aws_conn_id=YELLOW_MINIO_CONN_ID)
        s3_client = s3.get_conn()

        if not s3.check_for_key(key=CONFIG_FILE, bucket_name=SPARK_BUCKET):
            raise AirflowFailException(f"File {CONFIG_FILE} not found in bucket {SPARK_BUCKET}.")

        local_file_path = os.path.join(local_conf_directory, os.path.basename(CONFIG_FILE))
        s3_client.download_file(SPARK_BUCKET, CONFIG_FILE, local_file_path)

        logging.info("HOCON PARSING STARTED")
        config = hocon.ConfigFactory.parse_file(local_file_path)
        logging.info("HOCON PARSING COMPLETED")

        return config

    # ---- Config extraction ----

    def extract_config_info(self, sources_id_list: set[str] = None) -> None:
        """Populate `sources` (sources_id_list) and `storages` (always all
        storages)."""
        self.extract_source(sources_id_list=sources_id_list)

        have_storages = {storage.storage_id for storage in self.storages}
        for storage in self.raw_config.get("datalake.storages"):
            if storage["id"] not in have_storages:
                self.storages.append(StorageConf(storage_id=storage["id"], path=storage["path"]))

    def extract_source(self, sources_id_list: set[str]) -> None:
        """Append the corresponding ``datalake.sources`` entries whose ``id`` is in ``sources_id_list`` to
        `sources`.
        Logs the requested source ids for visibility. Raises ``AirflowFailException`` if no
        requested id matches any configured source.
        """
        import logging
        from airflow.exceptions import AirflowFailException

        raw_sources = self.raw_config.get("datalake.sources")

        logging.info("extract_source: requested %d source id(s): %s",
                     len(sources_id_list), sorted(sources_id_list))

        selected = [s for s in raw_sources if s["id"] in sources_id_list]
        if not selected:
            raise AirflowFailException(
                "extract_source: no requested source id matched datalake.sources "
                f"(requested: {sorted(sources_id_list)}).")

        # Avoid duplicates if ever extract_source is called multiple times with overlapping selectors.
        have_sources = {source.source_id for source in self.sources}
        self.sources.extend(
            SourceConf(source_id=s["id"], storage_id=s["storageid"], relative_path=s["path"])
            for s in selected if s["id"] not in have_sources
        )

    # ---- lookups & paths ----

    def get_source(self, source_id: str) -> SourceConf:
        """Return the populated SourceConf with this id"""
        for source in self.sources:
            if source.source_id == source_id:
                return source
        from airflow.exceptions import AirflowFailException
        raise AirflowFailException(
            f"Source ID {source_id} not found. Populate it first via extract_config_info().")

    def get_storage(self, storage_id: str) -> StorageConf:
        """Return the populated StorageConf with this id (raises if not populated)."""
        for storage in self.storages:
            if storage.storage_id == storage_id:
                return storage
        from airflow.exceptions import AirflowFailException
        raise AirflowFailException(f"Storage ID {storage_id} not found.")

    def source_s3_path(self, source_id: str, scheme: str = "s3a") -> str:
        """Full s3 URI of a dataset, given its source id."""
        source = self.get_source(source_id)
        return source.source_s3_path(self.get_storage(source.storage_id), scheme)

    def bucket_for_source(self, source_id: str) -> str:
        """Bucket name of the storage backing a given source id."""
        source = self.get_source(source_id)
        return self.get_storage(source.storage_id).bucket

    def released_bucket(self, resource_code: str) -> str:
        """Bucket of the ``released_<resource_code>`` source (the input bucket when publishing).

        Walks ``self.raw_config`` directly, since the released source is not one of the
        ``published_*`` sources populated for the publish flow."""
        from airflow.exceptions import AirflowFailException

        for source in self.raw_config.get("datalake.sources"):
            if f"released_{resource_code}" in source["id"]:
                storage_id = source["storageid"]
                for storage in self.raw_config.get("datalake.storages"):
                    if storage["id"] == storage_id:
                        return storage["path"].split("/")[2]

        raise AirflowFailException(
            f"Resource code {resource_code} does not have any released configurations.")

    # ---- cross-task serialization ----

    def to_dict(self) -> dict:
        """Converts DatalakeConfig class instance to its primitive, XCom-serializable form."""
        return {
            "minio_conn_id": self.minio_conn_id,
            "sources": [source.to_dict() for source in self.sources],
            "storages": [storage.to_dict() for storage in self.storages],
        }

    @classmethod
    def from_dict(cls, data: dict) -> DatalakeConfig:
        """
        Alternative constructor for DatalakeConfig that rebuilds an instance from a dictionary produced by `to_dict`.

        Rebuild a DatalakeConfig from `to_dict` mini-config output, without any S3 I/O.

        The result has ``raw_config = None``; lookups (`get_source`, `source_s3_path`,
        `bucket_for_source`) work, but raw-config methods (`extract_config_info`,
        `released_bucket`, `extract_publish_config_info`) do NOT work.

        The mini-config is intended for XCom serialization between tasks, and the subsequent
        tasks should only need a read-only view of the sources/storages that were populated in the first task.
        """
        obj = cls.__new__(cls)  # bypass __init__ → no S3 download
        obj.minio_conn_id = data.get("minio_conn_id")
        obj.raw_config = None
        obj.sources = [SourceConf.from_dict(x) for x in data.get("sources", [])]
        obj.storages = [StorageConf.from_dict(x) for x in data.get("storages", [])]
        return obj

    # ---- publish flow ----

    def extract_publish_config_info(self, resource_code: str, version_to_publish: str) -> dict:
        """
        Retrieve the table names, S3 paths (WITHOUT their extension), and bucket IDs needed for
        publishing data. The structure of the return dictionary is as follows:
        {
            "sources": {
                "source_id1": {
                    "output_bucket": <bucket_name>,
                    "output_path": <s3_path>,
                    "table": <table_name>
                },
                "source_id2": {
                    ...,
                }
            }
            ...,
            "clinical_bucket": str - Name of the clinical bucket, if it exists, otherwise None.
            "nominative_bucket": str - Name of the nominative bucket, if it exists, otherwise None.
            "input_bucket": <bucket_name> - The bucket from which the data will be published.
        }

        :param resource_code: Resource code of the project to publish.
        :param version_to_publish: Version of the project to publish.
        :returns: Dictionary with the source IDs, input & output buckets, output paths, table names.
        """
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from lib.config import PUBLISHED_BUCKET
        from lib.publish_utils import determine_minio_conn_id_from_config

        mini_config = {
            "input_bucket": None,
            "clinical_bucket": None,
            "nominative_bucket": None,
            "sources": {},
        }

        input_bucket = self.released_bucket(resource_code)
        mini_config["input_bucket"] = input_bucket

        chosen_conn_id = determine_minio_conn_id_from_config(self.minio_conn_id, input_bucket)
        s3 = S3Hook(aws_conn_id=chosen_conn_id)

        released_path = f"released/{resource_code}/{version_to_publish}/"
        table_paths = s3.list_prefixes(input_bucket, released_path, "/")

        # Populate the structured config for the published datasets we just discovered.
        published_source_ids = {
            f"published_{resource_code}_{table_path.split('/')[-2]}" for table_path in table_paths
        }
        self.extract_config_info(sources_id_list=published_source_ids)

        for table_path in table_paths:
            table = table_path.split("/")[-2]  # Extract table name; assumes a consistent structure
            source_id = f"published_{resource_code}_{table}"

            output_bucket = self.bucket_for_source(source_id)
            if "clinical" in output_bucket:
                mini_config["clinical_bucket"] = output_bucket
            if "nominative" in output_bucket:
                mini_config["nominative_bucket"] = output_bucket

            output_path = self.get_source(source_id).relative_path
            # Replace the version in the filename with the underscore version to publish
            output_path = output_path.replace("_{{version}}", f"_{version_to_publish.replace('-', '_')}")
            # Replace the version template for the folder with the dashed version to publish
            output_path = output_path.replace("{{version}}", version_to_publish)
            # If the output bucket is the default green bucket, place the output in the published folder
            if output_bucket == PUBLISHED_BUCKET:
                output_path = f"published{output_path}"

            mini_config["sources"][source_id] = {
                "output_bucket": output_bucket,
                "output_path": output_path,
                "table": table,
            }

        return mini_config
