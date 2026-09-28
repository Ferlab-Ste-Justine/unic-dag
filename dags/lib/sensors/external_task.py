"""
Cross-DAG gates. Wait for the upstream DAG's run covering the same interval to succeed,
matching it by ``data_interval_end`` so it works for both cron and dataset-triggered
upstreams and stays correct across reruns. ``lookback_days`` widens the match to upstreams
scheduled on an earlier day.
"""
from typing import Callable, Optional, Tuple

from pendulum import DateTime
from sqlalchemy.orm import Session

from airflow.models import DagRun
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.session import NEW_SESSION, provide_session

from lib.config import LOCAL_TZ

DEFAULT_POKE_INTERVAL = 60  # one minute
DEFAULT_TIMEOUT = 3600  # one hour


def _window(data_interval_end: DateTime, lookback_days: int) -> Tuple[DateTime, DateTime]:
    """Return [start, end) covering the interval-end day and the lookback_days before it."""
    day = data_interval_end.in_timezone(LOCAL_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    return day.subtract(days=lookback_days), day.add(days=1)


def _latest_run_execution_date(external_dag_id: str, lookback_days: int = 0) -> Callable[..., DateTime]:
    @provide_session
    def _fn(logical_date: DateTime, data_interval_end: Optional[DateTime] = None,  # pylint: disable=unused-argument
            session: Session = NEW_SESSION, **_) -> DateTime:
        # Match the latest upstream run whose interval ends within the window.
        # data_interval_end lands on the run's actual day and is stable across reruns.
        window_start, window_end = _window(data_interval_end, lookback_days)
        runs = session.query(DagRun).filter(
            DagRun.dag_id == external_dag_id,
            DagRun.data_interval_end >= window_start,
            DagRun.data_interval_end < window_end,
        ).all()
        # No run in the window yet: return the interval-end day we expect a run for, so the sensor waits for it.
        return max((run.execution_date for run in runs), default=window_end.subtract(days=1))
    return _fn


def wait_for(external_dag_id: str, *external_task_ids: str, external_task_group_id: Optional[str] = None,
             task_id: str, lookback_days: int = 0, poke_interval: int = DEFAULT_POKE_INTERVAL,
             timeout: int = DEFAULT_TIMEOUT) -> ExternalTaskSensor:
    """
    Sensor that succeeds once the latest upstream run within the lookback window succeeds.

    :param external_dag_id: upstream DAG to wait on
    :param external_task_ids: upstream task ids that must all succeed; omit to wait on the whole DAG
    :param external_task_group_id: wait on every task in this group instead of individual task ids
    :param task_id: id of this sensor task in the current DAG
    :param lookback_days: also match upstream runs from up to this many days before
    :param poke_interval: seconds between checks
    :param timeout: seconds before the sensor gives up and fails
    """
    return ExternalTaskSensor(
        task_id=task_id,
        external_dag_id=external_dag_id,
        external_task_ids=list(external_task_ids),
        external_task_group_id=external_task_group_id,
        execution_date_fn=_latest_run_execution_date(external_dag_id, lookback_days),
        allowed_states=["success"],
        failed_states=["failed"],
        # poke rather than reschedule: our 1-minute poke_interval is below what reschedule recommends
        mode="poke",
        poke_interval=poke_interval,
        timeout=timeout,
    )
