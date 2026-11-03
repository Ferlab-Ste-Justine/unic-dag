"""Unit tests for lib.tasks.qa."""
from datetime import datetime

from airflow import DAG

from lib.tasks.qa import test as qa_test


def _build_test(**kwargs):
    with DAG(dag_id="test_qa", start_date=datetime(2026, 1, 1), schedule=None) as dag:
        return qa_test("equal_counts", ["enriched_test_table"], "test", "yellow", "enriched", "config.conf",
                       "etl.jar", dag, **kwargs)


def test_qa_test_fails_on_failure_by_default():
    assert _build_test().skip_on_failure is False


def test_non_blocking_qa_test_skips_on_failure():
    assert _build_test(blocking=False).skip_on_failure is True
