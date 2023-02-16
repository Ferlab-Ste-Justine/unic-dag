"""
Ingestion and anonymized dags
"""
import os
import re
from datetime import timedelta, datetime

from airflow.models import Variable
from airflow.models.param import Param
from airflow import DAG

from core.default_args import generate_default_args
from core.slack import Slack
from spark_operators import read_json, setup_dag

DEFAULT_ARGS = generate_default_args(owner="unic", on_failure_callback=Slack.notify_task_failure)
DEFAULT_TIMEOUT_HOURS = 4

ROOT = Variable.get('dags_config', '/opt/airflow/dags/repo/dags/config')
EXTRACT_SCHEMA = '(.*)_config.json'
CONFIG_FILE = "config/prod.conf"

for (r, folders, files) in os.walk(ROOT):
    if r == ROOT:
        for namespace in folders:
            for configs in os.walk(f'{ROOT}/{namespace}'):
                for f in configs[2]:
                    schema = re.search(EXTRACT_SCHEMA, f).group(1)
                    dagid = f"{namespace}_{schema}".lower()
                    config = read_json(f"{ROOT}/{namespace}/{schema}_config.json")
                    k = 'timeout_hours'
                    timeout_hours = config[k] if k in config else DEFAULT_TIMEOUT_HOURS
                    dag = DAG(
                        dag_id=dagid,
                        schedule_interval=config['schedule'],
                        params={
                            "branch": Param("master", type="string"),
                            "version": Param("latest", type="string")
                        },
                        default_args=DEFAULT_ARGS,
                        start_date=datetime(2021, 1, 1),
                        concurrency=config['concurrency'],
                        catchup=False,
                        tags=[namespace],
                        dagrun_timeout=timedelta(hours=timeout_hours),
                        is_paused_upon_creation=True
                    )
                    with dag:
                        setup_dag(
                            dag=dag,
                            dag_config=config,
                            config_file=CONFIG_FILE,
                            jar='s3a://spark-prd/jars/unic-etl-{{ params.branch }}.jar',
                            schema=schema,
                            version='{{ params.version }}'
                        )
                    globals()[dagid] = dag
