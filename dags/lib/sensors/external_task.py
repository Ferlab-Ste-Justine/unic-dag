"""
Cross-DAG gates. Wait for an upstream DAG's task(s) to succeed in its run from the same day,
tolerating a dataset-triggered upstream whose logical date is not a clean cron date.
"""
from typing import Callable, Optional

from pendulum import DateTime
from sqlalchemy.orm import Session

from airflow.models import DagRun
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.session import NEW_SESSION, provide_session

from lib.config import LOCAL_TZ

DEFAULT_POKE_INTERVAL = 60  # one minute
DEFAULT_TIMEOUT = 3600  # one hour


def _same_day_execution_date(external_dag_id: str) -> Callable[..., DateTime]:
    @provide_session
    def _fn(logical_date: DateTime, data_interval_end: Optional[DateTime] = None,
            session: Session = NEW_SESSION, **_) -> DateTime:
        day = (data_interval_end or logical_date).in_timezone(LOCAL_TZ).replace(
            hour=0, minute=0, second=0, microsecond=0)
        runs = DagRun.find(dag_id=external_dag_id, execution_start_date=day,
                           execution_end_date=day.add(days=1), session=session)
        return max((run.execution_date for run in runs), default=logical_date)
    return _fn


def wait_for(external_dag_id: str, *external_task_ids: str, external_task_group_id: Optional[str] = None,
             task_id: str, poke_interval: int = DEFAULT_POKE_INTERVAL,
             timeout: int = DEFAULT_TIMEOUT) -> ExternalTaskSensor:
    """
    Sensor that succeeds once the upstream run from the same day succeeds.

    :param external_dag_id: upstream DAG to wait on
    :param external_task_ids: upstream task ids that must all succeed; omit to wait on the whole DAG
    :param external_task_group_id: wait on every task in this group instead of individual task ids
    :param task_id: id of this sensor task in the current DAG
    :param poke_interval: seconds between checks
    :param timeout: seconds before the sensor gives up and fails
    """
    return ExternalTaskSensor(
        task_id=task_id,
        external_dag_id=external_dag_id,
        external_task_ids=list(external_task_ids),
        external_task_group_id=external_task_group_id,
        execution_date_fn=_same_day_execution_date(external_dag_id),
        allowed_states=["success"],
        failed_states=["failed"],
        # poke rather than reschedule: our 1-minute poke_interval is below what reschedule recommends
        mode="poke",
        poke_interval=poke_interval,
        timeout=timeout,
    )
