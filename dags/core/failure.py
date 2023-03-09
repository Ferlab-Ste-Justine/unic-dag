from core.slack import Slack
from core.cleanup import Cleanup
import re

class Failure:
    def on_failure_callback(context):
        namespace = context['task'].namespace
        print(f"Operator Namespace: {namespace}")
        # name = context['task'].name
        name = context['task'].pod.metadata.name
        print(f"Operator Name: {name}")

        # #extract from exception
        # regex = name + "-.{32}"
        # pod_name = re.findall(regex, str(Exception))[0]
        # print(f"Pod Name: {pod_name}")

        Cleanup.cleanup_pods(name, namespace, is_failure=True)
        Slack.notify_task_failure(context)
