import json
import urllib.parse
import requests
from typing import Any
from airflow.models import Variable

class Slack:

    # SUCCESS = ':large_green_square:'
    INFO = ':large_blue_square:'
    # WARNING = ':large_orange_square:'
    ERROR = ':large_red_square:'

    base_url = Variable.get('base_url', None)
    slack_hook_url = Variable.get('slack_hook_url', None)

    def notify(markdown: str, type=INFO):
        if Slack.slack_hook_url:
            airflow_link = f' *[*<{Slack.base_url}|Airflow>*]*' if Slack.base_url else ''
            Slack.http_post(Slack.slack_hook_url, {
                'blocks': [
                    {
                        'type': 'section',
                        'text': {
                            'type': 'mrkdwn',
                            'text': f'{type} *PROD*{airflow_link} {markdown}',
                        },
                    },
                ],
            })

    def notify_task_failure(context):
        dag_id = context['dag'].dag_id
        task_id = context['task'].task_id

        slack_msg = """
            :red_circle: Task Failure. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
            task=task_id,
            dag=dag_id,
            exec_date=context['execution_date'],
            log_url=context['task_instance'].log_url,
        )

        slack_msg_json = json.dumps(slack_msg, indent=4)
        slack_msg_formatted = f'```{slack_msg_json}```'

        dag_link = Slack._dag_link(
            f'{dag_id}.{task_id}', dag_id, context['run_id'], task_id,
        )
        Slack.notify(f'Task {dag_link} failed.{slack_msg_formatted}', type=Slack.ERROR)

    # def notify_dag_start(context):
    #     dag_id = context['dag'].dag_id
    #     dag_link = Slack._dag_link(dag_id, dag_id, context['run_id'])
    #     dag_params = Slack._dag_params(context['params'])
    #     Slack.notify(f'DAG {dag_link} started.{dag_params}', Slack.INFO)
    #
    # def notify_dag_completion(context):
    #     dag_id = context['dag'].dag_id
    #     dag_link = Slack._dag_link(dag_id, dag_id, context['run_id'])
    #     dag_params = Slack._dag_params(context['params'])
    #     Slack.notify(f'DAG {dag_link} completed.{dag_params}', Slack.SUCCESS)

    def _dag_link(text: str, dag_id: str, run_id: str = '', task_id: str = ''):
        if Slack.base_url:
            params = urllib.parse.urlencode({
                'dag_run_id': run_id,
                'task_id': task_id
            })
            return f'<{Slack.base_url}/dags/{dag_id}/grid?{params}|{text}>'
        return text

    # def _dag_params(params: dict):
    #     if params:
    #         params_json = json.dumps(params, indent=4)
    #         return f'```{params_json}```'
    #     return ''

    def http_post(url: str, json: Any = None) -> requests.Response:
        with requests.post(url, json=json) as response:
            response.raise_for_status()
            return response
