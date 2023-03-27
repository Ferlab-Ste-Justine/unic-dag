"""
Ingestion and anonymized dags
"""
import os
import re
from datetime import timedelta, datetime

from airflow import DAG

from core.config import root, extract_schema, default_timeout_hours, default_args, config_file, spark_failure_msg, jar, \
    version, default_params
from spark_operators import read_json, setup_dag

for (r, folders, files) in os.walk(root):
    if r == root:
        for namespace in folders:
            for configs in os.walk(f'{root}/{namespace}'):
                for f in configs[2]:
                    schema = re.search(extract_schema, f).group(1)
                    dagid = f"{namespace}_{schema}".lower()
                    config = read_json(f"{root}/{namespace}/{schema}_config.json")
                    k = 'timeout_hours'
                    timeout_hours = config[k] if k in config else default_timeout_hours
                    exec_timeout_hours = 3/4 * timeout_hours
                    default_args["execution_timeout"] = timedelta(hours=exec_timeout_hours)
                    dag = DAG(
                        dag_id=dagid,
                        schedule_interval=config['schedule'],
                        params=default_params,
                        default_args=default_args,
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
                            config_file=config_file,
                            jar=jar,
                            schema=schema,
                            version=version,
                            spark_failure_msg=spark_failure_msg
                        )
                    globals()[dagid] = dag
