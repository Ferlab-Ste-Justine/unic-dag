"""
Ingestion and anonymized dags
"""
import os
import re
from datetime import timedelta, datetime

import pendulum
from airflow import DAG

from lib.config import dags_config_path, extract_resource, default_timeout_hours, default_args, config_file, \
    spark_failure_msg, jar, version, default_params
from lib.slack import Slack
from spark_operators import read_json, setup_dag

for (r, zones, _) in os.walk(dags_config_path):
    if r == dags_config_path:
        for zone in zones:
            for (_, subzones, _) in os.walk(f'{dags_config_path}/{zone}'):
                for subzone in subzones:
                    for (_, _, files) in os.walk(f'{dags_config_path}/{zone}/{subzone}'):
                        for f in files:
                            resource = re.search(extract_resource, f).group(1)
                            dagid = f"{subzone}_{resource}".lower()
                            config = read_json(f"{dags_config_path}/{zone}/{subzone}/{resource}_config.json")
                            k = 'timeout_hours'
                            timeout_hours = config[k] if k in config else default_timeout_hours
                            exec_timeout_hours = 3/4 * timeout_hours
                            default_args["execution_timeout"] = timedelta(hours=exec_timeout_hours)
                            dag = DAG(
                                dag_id=dagid,
                                schedule_interval=config['schedule'],
                                params=default_params,
                                default_args=default_args,
                                start_date=datetime(2021, 1, 1, tzinfo=pendulum.timezone("America/Montreal")),
                                concurrency=config['concurrency'],
                                catchup=False,
                                tags=[subzone],
                                dagrun_timeout=timedelta(hours=timeout_hours),
                                is_paused_upon_creation=True,
                                on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
                            )
                            with dag:
                                setup_dag(
                                    dag=dag,
                                    dag_config=config,
                                    config_file=config_file,
                                    jar=jar,
                                    resource=resource,
                                    version=version,
                                    spark_failure_msg=spark_failure_msg
                                )
                            globals()[dagid] = dag
