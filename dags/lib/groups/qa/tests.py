from typing import List, Dict

from airflow import DAG
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup

from lib.tasks import qa


@task_group(group_id="tests")
def tests(test_confs: List[Dict], resource: str, zone: str, subzone: str,
          config_file: str, jar: str, dag: DAG) -> TaskGroup:
    for pre_test in test_confs:
        pre_test_name = pre_test['name']
        pre_test_destinations = pre_test['destinations']
        qa.test(pre_test_name, pre_test_destinations, resource, zone, subzone, config_file, jar, dag)
