"""Unit tests for lib.publish_utils pure helpers.

Covers `add_extension_to_path` and the Minio connection-id routing in
`determine_minio_conn_id_from_config` across all input/output bucket branches.
"""
import pytest

from lib.config import (
    CATALOG_BUCKET,
    GREEN_MINIO_CONN_ID,
    NOMINATIVE_BUCKET,
    PUBLISHED_BUCKET,
    RED_MINIO_CONN_ID,
    RELEASED_BUCKET,
    VNA_CLINIQUE_RED_BUCKET,
    VNA_CLINIQUE_YELLOW_BUCKET,
    YELLOW_MINIO_CONN_ID,
)
from lib.publish_utils import (
    FileType,
    add_extension_to_path,
    determine_minio_conn_id_from_config,
)

DEFAULT_CONN = "default_minio_conn"


@pytest.mark.parametrize("output_type, expected", [
    (FileType.EXCEL, "some/path/file.xlsx"),
    (FileType.PARQUET, "some/path/file.parquet"),
])
def test_add_extension_to_path(output_type, expected):
    assert add_extension_to_path("some/path/file", output_type) == expected


@pytest.mark.parametrize("input_bucket, output_bucket, expected", [
    # input bucket only
    (RELEASED_BUCKET, None, GREEN_MINIO_CONN_ID),
    (CATALOG_BUCKET, None, YELLOW_MINIO_CONN_ID),
    (NOMINATIVE_BUCKET, None, RED_MINIO_CONN_ID),
    ("some_unknown_bucket", None, DEFAULT_CONN),
    (None, None, DEFAULT_CONN),
    # output bucket provided
    (None, "green-prd-clinical", GREEN_MINIO_CONN_ID),
    (RELEASED_BUCKET, "green-prd-clinical", GREEN_MINIO_CONN_ID),
    (CATALOG_BUCKET, "yellow-prd-clinical", YELLOW_MINIO_CONN_ID),
    (None, "red-prd-nominative", RED_MINIO_CONN_ID),
    (None, "some-other-bucket", DEFAULT_CONN),
    # imaging buckets
    (VNA_CLINIQUE_RED_BUCKET, None, RED_MINIO_CONN_ID),
    (VNA_CLINIQUE_YELLOW_BUCKET, None, YELLOW_MINIO_CONN_ID),
    # published buckets given as the input bucket, matched on their zone marker
    ("published-clinical-cpip", None, GREEN_MINIO_CONN_ID),
    ("published-nominative-cpip", None, RED_MINIO_CONN_ID),
    # the output bucket is weighed alongside the input, so the widest zone of the two wins
    (NOMINATIVE_BUCKET, "published-clinical-epqneuro", RED_MINIO_CONN_ID),
    (RELEASED_BUCKET, "published-nominative-bronchiolite", RED_MINIO_CONN_ID),
    # the default green published bucket carries no zone marker, and resolves on its name alone
    (RELEASED_BUCKET, PUBLISHED_BUCKET, GREEN_MINIO_CONN_ID),
])
def test_determine_minio_conn_id_from_config(input_bucket, output_bucket, expected):
    assert determine_minio_conn_id_from_config(
        DEFAULT_CONN, input_bucket, output_bucket=output_bucket) == expected


@pytest.mark.parametrize("input_buckets, expected", [
    # no bucket at all: DatalakeConfig.__init__ with no sources populated
    ((), DEFAULT_CONN),
    # several buckets: the widest zone wins
    ((RELEASED_BUCKET, CATALOG_BUCKET), YELLOW_MINIO_CONN_ID),
    ((RELEASED_BUCKET, NOMINATIVE_BUCKET), RED_MINIO_CONN_ID),
    ((CATALOG_BUCKET, NOMINATIVE_BUCKET, RELEASED_BUCKET), RED_MINIO_CONN_ID),
    # one curated input and three outputs, all nominative
    ((NOMINATIVE_BUCKET,) * 4, RED_MINIO_CONN_ID),
    # unknown buckets contribute no zone, so they never widen and never shadow a known one
    (("some_unknown_bucket", CATALOG_BUCKET), YELLOW_MINIO_CONN_ID),
    (("some_unknown_bucket", "another_unknown_bucket"), DEFAULT_CONN),
])
def test_determine_minio_conn_id_from_config_folds_input_buckets(input_buckets, expected):
    assert determine_minio_conn_id_from_config(DEFAULT_CONN, *input_buckets) == expected
