from core.slack import Slack
from core.cleanup import Cleanup
import re

class Failure:
    def on_failure_callback(context):
        namespace = context['task'].namespace
        exception = context['exception']
        name = context['task'].name

        #extract from exception
        regex = name + "-.{32}"
        pod_name = re.findall(regex, str(exception))[0]
        print(f"Pod Name: {pod_name}")

        Cleanup.cleanup_pods(pod_name, namespace, is_failure=True)
        Slack.notify_task_failure(context)
