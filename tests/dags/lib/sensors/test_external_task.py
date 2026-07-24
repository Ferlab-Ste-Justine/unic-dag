# pylint: disable=redefined-outer-name
"""
Unit tests for the cross-DAG wait_for sensor helper.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock

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


def _run(execution_date):
    return SimpleNamespace(execution_date=execution_date)


@pytest.fixture
def make_session():
    """Build a mock DB session whose day-window query returns the given runs."""
    def _make(runs):
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = runs
        return session
    return _make


def test_same_day_execution_date(make_session):
    """
    It should return the latest run's execution date within the day window.
    """
    session = make_session([_run(pendulum.datetime(2026, 7, 3, 10, tz=LOCAL_TZ)),
                            _run(pendulum.datetime(2026, 7, 3, 13, tz=LOCAL_TZ))])
    result = _same_day_execution_date("warehouse_unic")(
        logical_date=LOGICAL_DATE, data_interval_end=DATA_INTERVAL_END, session=session)
    assert result == pendulum.datetime(2026, 7, 3, 13, tz=LOCAL_TZ)


def test_same_day_execution_date_no_run(make_session):
    """
    It should return the interval-end day when the window has no run, so the sensor keeps waiting.
    """
    session = make_session([])
    result = _same_day_execution_date("warehouse_unic")(
        logical_date=LOGICAL_DATE, data_interval_end=DATA_INTERVAL_END, session=session)
    assert result == pendulum.datetime(2026, 7, 3, tz=LOCAL_TZ)


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
