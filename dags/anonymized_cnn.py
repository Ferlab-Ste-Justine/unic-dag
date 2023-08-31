"""
cnn anonymization DAG
"""
# pylint: disable=missing-function-docstring, duplicate-code
from datetime import timedelta, datetime

from airflow import DAG

from core.config import config_file, jar, version, spark_failure_msg, default_args
from spark_operators import setup_dag

config = {
    "concurrency": 2,
    "schedule": None,
    "timeout_hours": 1,
    "steps": [{
        "namespace": "anonymized",
        "main_class": "bio.ferlab.ui.etl.yellow.anonymized.Main",
        "publish_class": "",
        "schemas": [],
        "datasets":
            [
                {"dataset_id": "anonymized_cnn_admission"          , "cluster_type": "xsmall", "run_type": "default", "cluster_specs": {}, "dependencies": []},
                {"dataset_id": "anonymized_cnn_resuscitation"      , "cluster_type": "xsmall", "run_type": "default", "cluster_specs": {}, "dependencies": []},
            ]
    }]
}

with DAG(
    dag_id="anonymized_cnn",
    schedule_interval=config['schedule'],
    default_args=default_args,
    start_date=datetime(2023, 08, 31),
    concurrency=config['concurrency'],
    catchup=False,
    tags=["anonymized"],
    dagrun_timeout=timedelta(hours=config['timeout_hours']),
    is_paused_upon_creation=True
) as dag:

    setup_dag(
        dag=dag,
        dag_config=config,
        config_file=config_file,
        jar=jar,
        version=version,
        spark_failure_msg=spark_failure_msg,
    )
