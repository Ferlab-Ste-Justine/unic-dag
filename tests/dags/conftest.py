# pylint: disable=import-outside-toplevel
from pathlib import Path

import pytest
from airflow.models import DagBag


DAGS_DIR = Path(__file__).parents[2] / 'dags'

@pytest.fixture(scope='session')
def dag_bag():
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


@pytest.fixture
def fake_s3_hook(monkeypatch):
    """Patch S3Hook with a recording double; call with the keys list_keys should return, get the recorder."""
    import airflow.providers.amazon.aws.hooks.s3 as s3mod

    def _install(list_keys_result):
        captured = {}

        class _FakeS3Hook:
            def __init__(self, aws_conn_id=None):
                captured["conn_id"] = aws_conn_id

            def list_keys(self, bucket_name=None, prefix=None):
                captured["list"] = (bucket_name, prefix)
                return list(list_keys_result)

            def delete_objects(self, bucket=None, keys=None):
                captured.setdefault("deleted", []).append((bucket, list(keys)))

        monkeypatch.setattr(s3mod, "S3Hook", _FakeS3Hook)
        return captured
    return _install


@pytest.fixture
def fake_write_delta(monkeypatch):
    """Stub is_deltatable + capture polars write_delta's options; call with the is_deltatable bool."""
    def _install(is_deltatable):
        import deltalake
        import polars as pl

        monkeypatch.setattr(deltalake.DeltaTable, "is_deltatable",
                            staticmethod(lambda table_uri, storage_options=None: is_deltatable))
        captured = {}

        def _fake(*_args, **kwargs):
            captured["options"] = kwargs["delta_write_options"]

        monkeypatch.setattr(pl.DataFrame, "write_delta", _fake)
        return captured
    return _install
