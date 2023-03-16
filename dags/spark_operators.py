"""
Help class containing custom SparkKubernetesOperator
"""
import json
import re
from airflow import DAG
from airflow.operators.empty import EmptyOperator

from core.slack import Slack
from operators.spark import SparkOperator


def sanitize_string(string: str, replace_by: str):
    """
    Replace all special character in a string into another character
    :param string: string to be sanitized
    :param replace_by: replacement character
    :return: sanitized string
    """
    return re.sub("[^a-zA-Z0-9 ]", replace_by, string)


def get_start_operator(namespace: str,
                       schema: str):
    """
    :param namespace:
    :param dag:
    :param schema:
    :return:
    """
    return EmptyOperator(
        task_id=f"start_{namespace}_{schema}",
        on_execute_callback=Slack.notify_dag_start
    )


# pylint: disable=too-many-locals,no-else-return
def get_publish_operator(dag_config: dict,
                         config_file: str,
                         jar: str,
                         dag: DAG,
                         schema: str,
                         spark_failure_msg: str):
    """
    Create a publish task based on the config publish_class

    :param dag_config:
    :param config_file:
    :param jar:
    :param dag:
    :param schema:
    :param spark_failure_msg:
    :return: both start and end to the publish operator if the operator contain multiple task
    """
    namespace = dag_config['namespace']
    schemas = dag_config['schemas']
    main_class = dag_config['publish_class']

    if main_class == "bio.ferlab.ui.etl.red.raw.UpdateLog":
        job_id = f"log_update_{'_'.join(schemas)[:20].lower()}"
        args = [config_file, "journalisation.ETL_Truncate_Table", "set", *schemas]

    elif main_class == "bio.ferlab.ui.etl.green.published.coda.PublishToAidbox":
        job_id = "publish_to_aidbox"
        args = [config_file, *schemas]

    else:
        publish = EmptyOperator(
            task_id=f"publish_{namespace}_{schema}",
            on_success_callback=Slack.notify_dag_completion
        )

        return publish

    pod_name = sanitize_string(job_id, '-')

    job = SparkOperator(
        task_id=job_id,
        name=pod_name,
        arguments=args,
        namespace=namespace,
        spark_class=main_class,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="xsmall-etl",
        on_success_callback=Slack.notify_dag_completion,
        dag=dag
    )

    return job

def setup_dag(dag: DAG,
              dag_config: dict,
              config_file: str,
              jar: str,
              schema: str,
              version: str,
              spark_failure_msg: str):
    """
    setup a dag
    :param dag:
    :param dag_config:
    :param config_file:
    :param jar:
    :param schema:
    :param version: Version to release, defaults to "latest"
    :param spark_failure_msg:
    :return:
    """

    previous_publish = None

    for step_config in dag_config['steps']:
        start = get_start_operator(step_config['namespace'], schema)
        publish = get_publish_operator(step_config, config_file, jar, dag, schema, spark_failure_msg)

        if previous_publish:
            previous_publish >> start
        previous_publish = publish

        jobs = {}
        all_dependencies = []

        for conf in step_config['datasets']:
            dataset_id = conf['dataset_id']
            namespace = step_config['namespace']
            main_class = step_config['main_class']
            config_type = conf['cluster_type']
            run_type = conf['run_type']

            job = create_spark_job(dataset_id, namespace, run_type, config_type,
                                   config_file, jar, dag, main_class, version, spark_failure_msg)

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


def get_main_class(namespace: str, main_class: str):
    """
    Return the default main class for the namespace if no main class is provided

    :param namespace: Kubernetes namespace
    :param main_class: main class provided in config file
    :return: main class
    """
    main_classes = {
        "ingestion": "bio.ferlab.ui.etl.red.raw.Main",
        "curated": "bio.ferlab.ui.etl.red.curated.Main",
        "anonymized": "bio.ferlab.ui.etl.yellow.anonymized.Main",
        "released": "bio.ferlab.ui.etl.green.released.Main",
        "published": "bio.ferlab.ui.etl.green.published.Main",
    }
    if main_class != "":
        return main_class
    else:
        try:
            return main_classes[namespace]
        except KeyError as err:
            raise KeyError(f"No default main class for namespace: {namespace}. "
                           f"Please provide a main class in config file.") from err


def create_spark_job(destination: str,
                     namespace: str,
                     run_type: str,
                     cluster_type: str,
                     config_file: str,
                     jar: str,
                     dag: DAG,
                     main_class: str,
                     version: str,
                     spark_failure_msg: str):
    """
    create spark job operator
    :param destination:
    :param namespace:
    :param run_type:
    :param cluster_type:
    :param config_file:
    :param jar:
    :param dag:
    :param main_class:
    :param version: Version to release, defaults to "latest"
    :param spark_failure_msg:
    :return:
    """
    main_class = get_main_class(namespace, main_class)

    args = [config_file, run_type, destination]
    if namespace == "released":
        args.append(version)

    return SparkOperator(
        task_id=sanitize_string(destination, "_"),
        name=sanitize_string(destination[:40], '-'),
        namespace=namespace,
        arguments=args,
        spark_class=main_class,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config=f"{cluster_type}-etl",
        dag=dag
    )
