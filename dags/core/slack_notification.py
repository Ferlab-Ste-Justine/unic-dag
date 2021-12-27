from airflow.hooks.base import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

SLACK_CONN_ID = 'slack'
SLACK_WEBHOOK_TOKEN = BaseHook.get_connection(SLACK_CONN_ID).password


def task_fail_slack_alert(context):
    slack_msg = """
            :red_circle: Task Failure. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        exec_date=context.get('execution_date'),
        log_url=context.get('task_instance').log_url,
    )
    slack_kf_failure_notification = SlackWebhookOperator(
        task_id='slack_kf_failure_notification',
        http_conn_id=SLACK_CONN_ID,
        webhook_token=SLACK_WEBHOOK_TOKEN,
        message=slack_msg,
        username='slack-notification')
    return slack_kf_failure_notification.execute(context=context)


def task_success_slack_alert(context):
    slack_msg = """
            :large_green_circle: Task Success. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        exec_date=context.get('execution_date'),
        log_url=context.get('task_instance').log_url,
    )
    slack_kf_success_notification = SlackWebhookOperator(
        task_id='slack_kf_success_notification',
        http_conn_id=SLACK_CONN_ID,
        webhook_token=SLACK_WEBHOOK_TOKEN,
        message=slack_msg,
        username='slack-notification')
    return slack_kf_success_notification.execute(context=context)
