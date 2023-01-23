"""
Help class containing custom SparkKubernetesOperator
"""
import json
import re
from datetime import datetime

import yaml
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from operators.spark import SparkOperator

def sanitize_string(string: str, replace_by: str):
    """
    Replace all special character in a string into another character
    :param string: string to be sanitized
    :param replace_by: replacement character
    :return: sanitized string
    """
    return re.sub("[^a-zA-Z0-9 ]", replace_by, string)


def update_log_table(schemas: list,
                     log_table: str,
                     config_file: str,
                     main_class: str,
                     jar: str,
                     dag: DAG):
    """
    Create a SparkKubernetesOperator updating log table after ingestion job
    :param schemas:
    :param log_table:
    :param config_file:
    :param main_class:
    :param jar:
    :param image: The spark-operator Docker image
    :param dag:
    :return:
    """
    job_id = f"log_update_{'_'.join(schemas)[:20].lower()}"
    pod_name = sanitize_string(job_id, '-')

    job = SparkOperator(
        task_id=job_id,
        name=pod_name,
        arguments=[config_file, log_table, "set"] + schemas,
        namespace="ingestion",
        spark_class=main_class,
        spark_jar=jar,
        spark_config="small-etl",
        dag=dag
    )

    return job


def get_start_operator(namespace: str,
                      dag: DAG,
                      schema: str):
    """
    :param namespace:
    :param dag:
    :param schema:
    :return:
    """
    return DummyOperator(
        task_id=f"start_{namespace}_{schema}",
        dag=dag
    )


# pylint: disable=too-many-locals,no-else-return
def get_publish_operator(dag_config: dict,
                        etl_config_file: str,
                        jar: str,
                        dag: DAG,
                        schema: str):
    """
    Create a publish task based on the config publish_class
    :param dag_config:
    :param etl_config_file:
    :param jar:
    :param dag:
    :param image: The spark-operator Docker image
    :param schema:
    :return: both start and end to the publish operator if the operator contain multiple task
    """
    if dag_config['publish_class'] == "bio.ferlab.ui.etl.red.raw.UpdateLog":
        return update_log_table(dag_config['schemas'],
                                "journalisation.ETL_Truncate_Table",
                                etl_config_file,
                                dag_config['publish_class'],
                                jar,
                                dag
                                )
    else:
        publish = DummyOperator(
            task_id=f"publish_{dag_config['namespace']}_{schema}",
            dag=dag
        )
        return publish


def setup_dag(dag: DAG,
              dag_config: dict,
              etl_config_file: str,
              jar: str,
              image: str,
              schema: str,
              version: str):
    """
    setup a dag
    :param dag:
    :param dag_config:
    :param etl_config_file:
    :param jar:
    :param image: The spark-operator Docker image
    :param schema:
    :param version: Version to release, defaults to "latest"
    :return:
    """

    previous_publish = None

    for step_config in dag_config['steps']:
        start = get_start_operator(step_config['namespace'], dag, schema)
        publish = get_publish_operator(step_config, etl_config_file, jar, dag, schema)

        if previous_publish:
            previous_publish >> start
        previous_publish = publish

        jobs = {}
        all_dependencies = []

        for conf in step_config['datasets']:
            dataset_id = conf['dataset_id']
            namespace = step_config['namespace']
            spark_class = step_config['main_class']
            config_type = conf['cluster_type']
            run_type = conf['run_type']

            if namespace == 'released':
                arguments = [etl_config_file, run_type, dataset_id, version]
            else:
                arguments = [etl_config_file, run_type, dataset_id]

            print(f"NAMESPACE: {namespace}")

            job = SparkOperator(
                task_id=sanitize_string(f"create_{dataset_id}", "_"),
                name=sanitize_string(dataset_id[:40], '-'),
                namespace=namespace,
                arguments=arguments,
                spark_class=spark_class,
                spark_jar=jar,
                spark_config=f"{config_type}-etl",
                dag=dag
            )

            all_dependencies = all_dependencies + conf['dependencies']
            jobs[dataset_id] = {"job": job, "dependencies": conf['dependencies']}

        for dataset_id, job in jobs.items():
            for dependency in job['dependencies']:
                jobs[dependency]['job'] >> job['job']
            if len(job['dependencies']) == 0:
                start >> job['job']
            if dataset_id not in all_dependencies:
                job['job'] >> publish

def read_json(path: str):
    """
    read json file
    :param path:
    :return:
    """
    return json.load(open(path, encoding='UTF8'))

