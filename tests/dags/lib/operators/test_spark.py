"""Unit tests for lib.operators.spark.

Covers skip_on_failure: a failing task is skipped after notifying Slack and cleaning up its pods once,
while a task without it still fails.
"""
from types import SimpleNamespace

from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

from lib.operators import spark as spark_module
from lib.operators.spark import SparkOperator

SPARK_FAILURE_MSG = "Spark job failed"


def _operator(skip_on_failure):
    return SparkOperator(task_id="qa_test", spark_class="Main", spark_jar="etl.jar",
                         spark_failure_msg=SPARK_FAILURE_MSG, zone="yellow", skip_on_failure=skip_on_failure)


def _run(monkeypatch, task, pod_error=None, driver_failed=False):
    """
    Run an operator against a fake pod and return what was cleaned up and notified.

    :param pod_error: Exception raised by the client pod, as KubernetesPodOperator does when the pod fails.
    :param driver_failed: True if the Spark driver did not succeed: cleanup_pods then raises spark_failure_msg
                          after cleaning up, as it does in production.
    """
    events = []

    def _fake_execute(self, _context):
        self.pod = SimpleNamespace(metadata=SimpleNamespace(name="qa-test-pod", namespace="unic-yellow"))
        if pod_error:
            raise pod_error

    def _fake_cleanup_pods(name, _namespace, spark_failure_msg, failed=False):
        events.append(f"cleanup {name}")
        if driver_failed and not failed:
            raise AirflowFailException(spark_failure_msg)

    monkeypatch.setattr(KubernetesPodOperator, "execute", _fake_execute)
    monkeypatch.setattr(spark_module, "cleanup_pods", _fake_cleanup_pods)
    monkeypatch.setattr(spark_module.Slack, "notify_task_skip_on_failure", lambda context: events.append("slack"))

    try:
        task.execute({})
    except AirflowException as exception:
        return exception, events
    return None, events


def test_success_cleans_up_without_notification(monkeypatch):
    raised, events = _run(monkeypatch, _operator(skip_on_failure=True))

    assert raised is None
    assert events == ["cleanup qa-test-pod"]


def test_driver_failure_is_raised_without_skip_on_failure(monkeypatch):
    raised, events = _run(monkeypatch, _operator(skip_on_failure=False), driver_failed=True)

    assert isinstance(raised, AirflowFailException)
    assert events == ["cleanup qa-test-pod"]


def test_driver_failure_is_skipped_after_a_single_cleanup(monkeypatch):
    raised, events = _run(monkeypatch, _operator(skip_on_failure=True), driver_failed=True)

    assert isinstance(raised, AirflowSkipException)
    assert events == ["cleanup qa-test-pod", "slack"]


def test_pod_failure_is_skipped_after_cleanup_and_notification(monkeypatch):
    raised, events = _run(monkeypatch, _operator(skip_on_failure=True), pod_error=AirflowException("Pod returned a failure"))

    assert isinstance(raised, AirflowSkipException)
    assert events == ["cleanup qa-test-pod", "slack"]
