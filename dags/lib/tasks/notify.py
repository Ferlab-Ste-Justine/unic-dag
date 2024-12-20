from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from lib.slack import Slack


def start(task_id: str = "start", notify: bool = False) -> EmptyOperator:
    """
    Notify that the DAG has started.
    :param task_id: Task ID of the start task. Defaults to "start".
    :param notify: If True, sends a notification to Slack. Defaults to False.
    """
    on_execute_callback = Slack.notify_dag_start if notify else None
    return EmptyOperator(
        task_id=task_id,
        on_execute_callback=on_execute_callback,
        trigger_rule=TriggerRule.NONE_FAILED
    )


def end(task_id: str = "end", notify: bool = False) -> EmptyOperator:
    """
    Notify that the DAG has ended.
    :param task_id: Task ID of the end task. Defaults to "end".
    :param notify: If True, sends a notification to Slack. Defaults to False.
    """
    on_execute_callback = Slack.notify_dag_completion if notify else None
    return EmptyOperator(
        task_id=task_id,
        on_execute_callback=on_execute_callback,
        trigger_rule=TriggerRule.NONE_FAILED
    )
