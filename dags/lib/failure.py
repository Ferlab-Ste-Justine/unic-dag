import logging
import re

from lib.cleanup import cleanup_pods
from lib.slack import Slack


def task_on_failure_callback(context):
    exception = context['exception']
    namespace = context['task'].namespace
    name = context['task'].name
    spark_failure_msg = context['task'].spark_failure_msg

    # check if it is a spark failure, as the cleanup has already been done if a spark job fails.
    if str(exception) != spark_failure_msg:
        try:
            # extract pod_name from exception
            regex = name + "-.{32}"
            pod_name = re.findall(regex, str(exception))[0]

            cleanup_pods(pod_name, namespace, spark_failure_msg, failed=True)
        except IndexError:
            logging.error("Pod name not found. Unable to delete pods.")

    Slack.notify_task_failure(context)
