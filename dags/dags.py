"""
Ingestion and anonymized dags
"""
import os
import re
from airflow import DAG
from airflow.utils.dates import days_ago
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

for (r, folders, files) in os.walk(ROOT):
    if r == ROOT:
        print(r)
        print(folders)
        for namespace in folders:
            for configs in os.walk(f'{ROOT}/{namespace}'):
                for f in configs[2]:
                    schema = re.search(EXTRACT_SCHEMA, f).group(1)
                    dagid = f"{namespace}_{schema}".lower()
                    config = read_json(f"{ROOT}/{namespace}/{schema}_config.json")
                    dag = DAG(
                        dag_id=dagid,
                        schedule_interval=None,
                        default_args=DEFAULT_ARGS,
                        start_date=days_ago(2),
                        concurrency=config['concurrency'],
                        catchup=False,
                        tags=[namespace]
                    )
                    with dag:
                        setup_dag(dag, config, namespace, CONFIG_FILE)
                    globals()[dagid] = dag
