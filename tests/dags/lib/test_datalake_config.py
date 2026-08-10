"""Unit tests for lib.datalake_config value objects + DatalakeConfig read/selection logic.

All offline: tests use DatalakeConfig.from_dict (bypasses __init__, no S3) or build a bare instance via
object.__new__ with a fake raw_config, so nothing downloads/parses config/prod.conf.
"""
# pylint: disable=protected-access
import pytest

from airflow.exceptions import AirflowFailException

from lib.datalake_config import DatalakeConfig, SourceConf, StorageConf


def _cfg_from_raw(sources, storages):
    """A DatalakeConfig with a fake raw_config, bypassing __init__ (no S3)."""
    cfg = object.__new__(DatalakeConfig)
    cfg.sources = []
    cfg.storages = []
    cfg.raw_config = {"datalake.sources": sources, "datalake.storages": storages}
    return cfg


def _sample_mini_config():
    return {
        "minio_conn_id": "red_minio",
        "storages": [{"storage_id": "red", "path": "s3a://red-prd/curated"}],
        "sources": [{"source_id": "curated_softpath_hl7_oru_r01_obx",
                     "storage_id": "red", "relative_path": "hl7/obx"}],
    }


@pytest.mark.parametrize("path, expected_bucket", [
    ("s3a://red-prd", "red-prd"),
    ("s3a://red-prd/curated/x", "red-prd"),
    ("s3a://yellow-prd/a/b/c", "yellow-prd"),
])
def test_storage_bucket(path, expected_bucket):
    assert StorageConf(storage_id="s", path=path).bucket == expected_bucket


@pytest.mark.parametrize("path, scheme, expected", [
    ("s3a://red-prd/curated/x", "s3", "s3://red-prd/curated/x"),
    ("s3a://red-prd/curated/x/", "s3", "s3://red-prd/curated/x"),
    ("s3a://red-prd", "s3a", "s3a://red-prd"),
])
def test_storage_base_uri_reschemes(path, scheme, expected):
    assert StorageConf(storage_id="s", path=path).base_uri(scheme) == expected


def test_source_s3_path_composes_storage_and_relative_path():
    storage = StorageConf(storage_id="red", path="s3a://red-prd/curated")
    source = SourceConf(source_id="src", storage_id="red", relative_path="/hl7/reports")
    assert source.source_s3_path(storage, scheme="s3") == "s3://red-prd/curated/hl7/reports"


def test_from_dict_to_dict_round_trip_preserves_read_path():
    cfg = DatalakeConfig.from_dict(_sample_mini_config())
    again = DatalakeConfig.from_dict(cfg.to_dict())
    assert again.minio_conn_id == "red_minio"
    assert again.source_s3_path("curated_softpath_hl7_oru_r01_obx", scheme="s3") == \
        "s3://red-prd/curated/hl7/obx"
    assert again.bucket_for_source("curated_softpath_hl7_oru_r01_obx") == "red-prd"


def test_source_s3_path_and_bucket_for_source():
    cfg = DatalakeConfig.from_dict(_sample_mini_config())
    assert cfg.source_s3_path("curated_softpath_hl7_oru_r01_obx", scheme="s3") == \
        "s3://red-prd/curated/hl7/obx"
    assert cfg.bucket_for_source("curated_softpath_hl7_oru_r01_obx") == "red-prd"


def test_get_source_raises_on_unknown_id():
    cfg = DatalakeConfig.from_dict(_sample_mini_config())
    with pytest.raises(AirflowFailException):
        cfg.get_source("does_not_exist")


_RAW_SOURCES = [
    {"id": "curated_softpath_hl7_oru_r01_obx", "storageid": "red", "path": "hl7/obx"},
    {"id": "curated_softpath_hl7_obx_parsed_reports", "storageid": "red", "path": "hl7/reports"},
    {"id": "curated_softpath_hl7_obx_extracted_tables", "storageid": "red", "path": "hl7/tables"},
    {"id": "some_other_source", "storageid": "yellow", "path": "other/x"},
]
_RAW_STORAGES = [
    {"id": "red", "path": "s3a://red-prd/curated"},
    {"id": "yellow", "path": "s3a://yellow-prd/anon"},
]


def test_extract_source_selects_only_requested_ids():
    cfg = _cfg_from_raw(_RAW_SOURCES, _RAW_STORAGES)
    cfg.extract_source({"curated_softpath_hl7_oru_r01_obx",
                        "curated_softpath_hl7_obx_parsed_reports"})
    assert {s.source_id for s in cfg.sources} == {
        "curated_softpath_hl7_oru_r01_obx", "curated_softpath_hl7_obx_parsed_reports"}


def test_extract_source_raises_on_empty_selection():
    cfg = _cfg_from_raw(_RAW_SOURCES, _RAW_STORAGES)
    with pytest.raises(AirflowFailException):
        cfg.extract_source({"nonexistent_id"})


# selects the requested sources and populates only their backing storages
def test_extract_config_info():
    cfg = _cfg_from_raw(_RAW_SOURCES, _RAW_STORAGES)
    cfg.extract_config_info(sources_id_list={
        "curated_softpath_hl7_oru_r01_obx",
        "curated_softpath_hl7_obx_parsed_reports",
    })
    # both requested sources are extracted with their storage ref + relative path
    assert {(s.source_id, s.storage_id, s.relative_path) for s in cfg.sources} == {
        ("curated_softpath_hl7_oru_r01_obx", "red", "hl7/obx"),
        ("curated_softpath_hl7_obx_parsed_reports", "red", "hl7/reports"),
    }
    # only the backing storage (red) is populated, once, and the unreferenced yellow is excluded
    assert [(s.storage_id, s.path) for s in cfg.storages] == [("red", "s3a://red-prd/curated")]


def test_released_bucket_returns_storage_bucket():
    cfg = _cfg_from_raw(
        [{"id": "released_myproj_table_a", "storageid": "green", "path": "released/x"}],
        [{"id": "green", "path": "s3a://green-prd/released"}],
    )
    assert cfg.released_bucket("myproj") == "green-prd"


def test_released_bucket_raises_when_absent():
    cfg = _cfg_from_raw([], [])
    with pytest.raises(AirflowFailException):
        cfg.released_bucket("missing")
