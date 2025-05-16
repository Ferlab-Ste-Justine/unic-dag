"""
Create DAGs from JSON configuration files.
"""
import json
import os
import re
from datetime import timedelta

import pendulum
from airflow import DAG

from lib.config import DAGS_CONFIG_PATH, EXTRACT_RESOURCE, DEFAULT_TIMEOUT_HOURS, DEFAULT_ARGS, CONFIG_FILE, \
    SPARK_FAILURE_MSG, JAR, DEFAULT_PARAMS, DEFAULT_START_DATE, DEFAULT_CONCURRENCY
from lib.slack import Slack
from tasks import create_tasks

for (r, zones, _) in os.walk(DAGS_CONFIG_PATH):
    if r == DAGS_CONFIG_PATH:
        for zone in zones:
            for (_, subzones, _) in os.walk(f'{DAGS_CONFIG_PATH}/{zone}'):
                for subzone in subzones:
                    for (_, _, files) in os.walk(f'{DAGS_CONFIG_PATH}/{zone}/{subzone}'):
                        for f in files:
                            resource = re.search(EXTRACT_RESOURCE, f).group(1)
                            dagid = f"{subzone}_{resource}".lower()
                            with open(f"{DAGS_CONFIG_PATH}/{zone}/{subzone}/{f}", encoding='UTF8') as json_file:
                                config = json.load(json_file)

                                timeout_hours = config['timeout_hours'] if 'timeout_hours' in config else DEFAULT_TIMEOUT_HOURS
                                exec_timeout_hours = 3 / 4 * timeout_hours

                                start_date = pendulum.parse(config['start_date'], tz="America/Montreal") \
                                    if 'start_date' in config else DEFAULT_START_DATE

                                args = DEFAULT_ARGS.copy()
                                args["execution_timeout"] = timedelta(hours=exec_timeout_hours)

                                dag = DAG(
                                    dag_id=dagid,
                                    schedule_interval=config['schedule'] if 'schedule' in config else None,
                                    params=DEFAULT_PARAMS,
                                    default_args=args,
                                    start_date=start_date,
                                    concurrency=config['concurrency'] if 'concurrency' in config else DEFAULT_CONCURRENCY,
                                    catchup=config['catchup'] if 'catchup' in config else False,
                                    tags=[subzone],
                                    dagrun_timeout=timedelta(hours=timeout_hours),
                                    is_paused_upon_creation=True,
                                    on_failure_callback=Slack.notify_dag_failure
                                    # Should send notification to Slack when DAG exceeds timeout
                                )
                                with dag:
                                    create_tasks(
                                        dag=dag,
                                        dag_config=config,
                                        config_file=CONFIG_FILE,
                                        jar=JAR,
                                        resource=resource,
                                        spark_failure_msg=SPARK_FAILURE_MSG
                                    )
                                globals()[dagid] = dag
