# pylint: disable=redefined-outer-name
"""
Unit tests for the cross-DAG wait_for sensor helper.
"""
from types import SimpleNamespace
from unittest.mock import patch

import pendulum
import pytest

from airflow.sensors.external_task import ExternalTaskSensor
from lib.config import LOCAL_TZ
from lib.sensors.external_task import (
    DEFAULT_POKE_INTERVAL,
    DEFAULT_TIMEOUT,
    _same_day_execution_date,
    wait_for,
)

LOGICAL_DATE = pendulum.datetime(2026, 6, 5, 8, tz=LOCAL_TZ)
DATA_INTERVAL_END = pendulum.datetime(2026, 7, 3, 8, tz=LOCAL_TZ)
# @provide_session opens a real DB session only when none is passed; a dummy keeps the test offline
DUMMY_SESSION = object()


@pytest.fixture
def mock_find():
    with patch("lib.sensors.external_task.DagRun.find") as mock:
        yield mock


def _run(execution_date):
    return SimpleNamespace(execution_date=execution_date)


def test_same_day_execution_date(mock_find):
    """
    It should return the latest run's execution date within the day window.
    """
    mock_find.return_value = [_run(pendulum.datetime(2026, 7, 3, 10, tz=LOCAL_TZ)),
                             _run(pendulum.datetime(2026, 7, 3, 13, tz=LOCAL_TZ))]
    result = _same_day_execution_date("warehouse_unic")(
        logical_date=LOGICAL_DATE, data_interval_end=DATA_INTERVAL_END, session=DUMMY_SESSION)
    assert result == pendulum.datetime(2026, 7, 3, 13, tz=LOCAL_TZ)


def test_same_day_execution_date_no_run(mock_find):
    """
    It should fall back to logical_date when the window has no run.
    """
    mock_find.return_value = []
    result = _same_day_execution_date("warehouse_unic")(
        logical_date=LOGICAL_DATE, data_interval_end=DATA_INTERVAL_END, session=DUMMY_SESSION)
    assert result == LOGICAL_DATE


def test_same_day_execution_date_query(mock_find):
    """
    It should query a one-day window around data_interval_end.
    """
    mock_find.return_value = []
    _same_day_execution_date("warehouse_unic")(
        logical_date=LOGICAL_DATE, data_interval_end=DATA_INTERVAL_END, session=DUMMY_SESSION)
    kwargs = mock_find.call_args.kwargs
    assert kwargs["execution_start_date"] == pendulum.datetime(2026, 7, 3, tz=LOCAL_TZ)
    assert kwargs["execution_end_date"] == pendulum.datetime(2026, 7, 4, tz=LOCAL_TZ)


def test_wait_for_tasks():
    """
    It should wait for the given tasks when external_task_ids are passed.
    """
    sensor = wait_for("warehouse_unic", "warehouse.warehouse_lab_results",
                      "warehouse.warehouse_microbiology", task_id="wait")
    assert isinstance(sensor, ExternalTaskSensor)
    assert sensor.external_dag_id == "warehouse_unic"
    assert sensor.external_task_ids == ["warehouse.warehouse_lab_results", "warehouse.warehouse_microbiology"]
    assert sensor.mode == "poke"
    assert sensor.poke_interval == DEFAULT_POKE_INTERVAL
    assert sensor.timeout == DEFAULT_TIMEOUT


def test_wait_for_dag():
    """
    It should wait for the whole DAG when no tasks are passed.
    """
    sensor = wait_for("curated_unic", task_id="wait")
    assert sensor.external_task_ids is None


def test_wait_for_task_group():
    """
    It should wait for the group when external_task_group_id is passed.
    """
    sensor = wait_for("curated_unic", external_task_group_id="curated", task_id="wait")
    assert sensor.external_task_group_id == "curated"
    assert sensor.external_task_ids is None
