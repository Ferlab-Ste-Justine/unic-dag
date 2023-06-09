"""
Participant index anonymization DAG
"""
# pylint: disable=missing-function-docstring, duplicate-code
from datetime import timedelta, datetime

from airflow import DAG
from airflow.models import Param

from core.config import config_file, jar, version, spark_failure_msg, default_args, default_params
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
                {"dataset_id": "anonymized_unic_participant_index_cardiopathie", "cluster_type": "xsmall", "run_type": "default", "cluster_specs": {}, "dependencies": []},
                {"dataset_id": "anonymized_unic_participant_index_coprema"     , "cluster_type": "xsmall", "run_type": "default", "cluster_specs": {}, "dependencies": []},
                {"dataset_id": "anonymized_unic_participant_index_monchemin"   , "cluster_type": "xsmall", "run_type": "default", "cluster_specs": {}, "dependencies": []},
                {"dataset_id": "anonymized_unic_participant_index_signature"   , "cluster_type": "xsmall", "run_type": "default", "cluster_specs": {}, "dependencies": []},
                {"dataset_id": "anonymized_unic_participant_index_simapp"      , "cluster_type": "xsmall", "run_type": "default", "cluster_specs": {}, "dependencies": []},
            ]
    }]
}

# Add extra param
params = default_params.copy()
params.update({"dataset_id": Param("*", type="string")})

with DAG(
    dag_id="anonymized_participant_index",
    schedule_interval=config['schedule'],
    params=params,
    default_args=default_args,
    start_date=datetime(2021, 1, 1),
    concurrency=config['concurrency'],
    catchup=False,
    tags=["anonymized"],
    dagrun_timeout=timedelta(hours=config['timeout_hours']),
    is_paused_upon_creation=True
) as dag:

    def skip_task() -> str:
        return "{% if params.dataset_id == '*' or params.dataset_id == task.task_id %}{% else %}yes{% endif %}"

    setup_dag(
        dag=dag,
        dag_config=config,
        config_file=config_file,
        jar=jar,
        schema="participant_index",
        version=version,
        spark_failure_msg=spark_failure_msg,
        skip_task=skip_task()
    )
