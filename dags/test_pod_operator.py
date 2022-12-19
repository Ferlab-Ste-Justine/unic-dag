from airflow import DAG
from operators.spark import SparkOperator
from datetime import datetime

JAR = "s3a://spark-prd/jars/unic-etl-UNIC-875.jar"

with DAG(
        dag_id='test_pod_operator_default_new',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
) as dag:
    test_pod_operator = SparkOperator(
        task_id='test_pod_operator_default_new',
        name='test-pod-operator_default_new',
        namespace="raw",
        spark_class="bio.ferlab.ui.etl.experimental.TestClass",
        spark_jar=JAR,
        cmds=['echo', 'hello'],
        spark_config=f"xsmall-etl",
        dag=dag
    )
