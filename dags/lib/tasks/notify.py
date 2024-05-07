from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from lib.slack import Slack


def start(task_id: str = "start") -> EmptyOperator:
    """
    Notify that the DAG has started.
    :param task_id: Task ID of the start task. Defaults to "start".
    """
    return EmptyOperator(
        task_id=task_id,
        on_execute_callback=Slack.notify_dag_start,
        trigger_rule=TriggerRule.NONE_FAILED
    )


def end(task_id: str = "end") -> EmptyOperator:
    """
    Notify that the DAG has ended.
    :param task_id: Task ID of the end task. Defaults to "end".
    """
    return EmptyOperator(
        task_id=task_id,
        on_execute_callback=Slack.notify_dag_completion,
        trigger_rule=TriggerRule.NONE_FAILED
    )
