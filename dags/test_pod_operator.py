from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from operators.spark import SparkOperator
from datetime import datetime

JAR = "s3a://spark-prd/jars/unic-etl-{{ dag_run.conf.get('branch', 'UNIC-875') }}.jar"

with DAG(
        dag_id='test_pod_operator_default',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
) as dag:
    test_pod_operator_default = SparkOperator(
        task_id='test_pod_operator_default',
        name='test-pod-operator-default',
        namespace="ingestion",
        spark_class="bio.ferlab.ui.etl.experimental.TestClass",
        spark_jar=JAR,
        cmds=['echo', 'hello'],
        spark_config=f"xsmall-etl",
        dag=dag
    )
