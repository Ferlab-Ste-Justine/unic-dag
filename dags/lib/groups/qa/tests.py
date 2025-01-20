from typing import List, Dict

from airflow import DAG
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup

from lib.tasks import qa


@task_group(group_id="tests")
def tests(test_confs: List[Dict], resource: str, zone: str, subzone: str,
          config_file: str, jar: str, dag: DAG) -> TaskGroup:
    for test in test_confs:
        test_name = test['name']
        test_destinations = test['destinations']
        test_cluster_type = test['cluster_type']
        qa.test(test_name, test_destinations, resource, zone, subzone, config_file, jar, dag, cluster_type = test_cluster_type)
