"""
Ingestion and anonymized dags
"""
import os
import re
from datetime import datetime

from airflow import DAG

from spark_operators import read_json, setup_dag

# DEFAULT_ARGS = generate_default_args(owner="cbotek", on_failure_callback=task_fail_slack_alert)
DEFAULT_ARGS = {
    "owner": "cbotek",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "cbotek@ferlab.bio"
}

ROOT = '/opt/airflow/dags/repo/dags/config'
EXTRACT_SCHEMA = '(.*)_config.json'
CONFIG_FILE = "config/prod.conf"
JAR = "s3a://spark-prd/jars/unic-etl-{{ dag_run.conf.get('branch', 'master') }}.jar"
IMAGE = "ferlabcrsj/spark-operator:{{ dag_run.conf.get('imageVersion', '3.0.0_0') }}"

for (r, folders, files) in os.walk(ROOT):
    if r == ROOT:
        for namespace in folders:
            for configs in os.walk(f'{ROOT}/{namespace}'):
                for f in configs[2]:
                    schema = re.search(EXTRACT_SCHEMA, f).group(1)
                    dagid = f"{namespace}_{schema}".lower()
                    config = read_json(f"{ROOT}/{namespace}/{schema}_config.json")
                    dag = DAG(
                        dag_id=dagid,
                        schedule_interval=config['schedule'],
                        default_args=DEFAULT_ARGS,
                        start_date=datetime(2021, 1, 1),
                        concurrency=config['concurrency'],
                        catchup=False,
                        tags=[namespace]
                    )
                    with dag:
                        setup_dag(
                            dag=dag,
                            dag_config=config,
                            etl_config_file=CONFIG_FILE,
                            jar=JAR,
                            image=IMAGE,
                            schema=schema
                        )
                    globals()[dagid] = dag
