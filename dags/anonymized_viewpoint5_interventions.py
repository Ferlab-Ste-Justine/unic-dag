"""
Test DAG for vp5 interventions
"""
from datetime import datetime, timedelta

from airflow import DAG

from core.config import default_params, default_args, jar, spark_failure_msg
from operators.spark import SparkOperator

with DAG(
        dag_id="anonymized_viewpoint5_interventions",
        schedule_interval=None,
        params=default_params,
        default_args=default_args,
        start_date=datetime(2021, 1, 1),
        concurrency=1,
        catchup=False,
        tags=["anonymized"],
        dagrun_timeout=timedelta(hours=1),
        is_paused_upon_creation=True
) as dag:
    SparkOperator(
        task_id="anonymized_viewpoint_interventions_2006_2019",
        name="anonymized-viewpoint-interventions-2006-2019",
        arguments=[
            "--config", "config/prod.conf",
            "--steps", "initial",
            "--app-name", "anonymized_viewpoint_interventions_2006_2019",
            "--destination", "anonymized_viewpoint_interventions_2006_2019",
        ],
        zone="yellow",
        spark_class="bio.ferlab.ui.etl.yellow.anonymized.Main",
        spark_jar=f"--packages com.crealytics:spark-excel_2.12:3.3.1_0.18.7 {jar}",
        spark_failure_msg=spark_failure_msg,
        spark_config="small",
        dag=dag
    )
