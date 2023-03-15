from core.slack import Slack
from core.cleanup import Cleanup
import re

class Failure:
    def on_failure_callback(context):
        exception = context['exception']
        namespace = context['task'].namespace
        name = context['task'].name
        spark_failure_msg = context['task'].spark_failure_msg

        print(exception)
        print(str(exception))
        print(spark_failure_msg)

        # check if it is a spark failure, as the cleanup has already been done if a spark job fails.
        if str(exception) != spark_failure_msg:
            print("IN EXECUTION TIMEOUT FAILURE")
            try:
                # extract pod_name from exception
                regex = name + "-.{32}"
                pod_name = re.findall(regex, str(exception))[0]

                Cleanup.cleanup_pods(pod_name, namespace, spark_failure_msg, failed=True)
            except IndexError:
                print("Pod name not found. Unable to delete pods.")

        Slack.notify_task_failure(context)
