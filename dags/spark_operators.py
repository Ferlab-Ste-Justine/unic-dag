"""
Help class containing custom SparkKubernetesOperator
"""
import json
import re
from typing import Optional

from airflow import DAG
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from lib.config import spark_test_failure_msg
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks import notify


def sanitize_string(string: str, replace_by: str):
    """
    Replace all special character in a string into another character
    :param string: string to be sanitized
    :param replace_by: replacement character
    :return: sanitized string
    """
    return re.sub("[^a-zA-Z0-9 ]", replace_by, string)


# pylint: disable=too-many-locals,no-else-return
def get_publish_operator(dag_config: dict,
                         config_file: str,
                         jar: str,
                         dag: DAG,
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
    zone = dag_config['destination_zone']
    schemas = dag_config['schemas']
    main_class = dag_config['publish_class']

    if main_class == "bio.ferlab.ui.etl.red.raw.UpdateLog":
        job_id = f"log_update_{'_'.join(schemas)[:20].lower()}"
        args = [config_file, "journalisation.ETL_Truncate_Table", "set", *schemas]

    elif main_class == "bio.ferlab.ui.etl.green.published.coda.PublishToAidbox":
        job_id = "publish_to_aidbox"
        args = [config_file, *schemas]

    else:
        return None

    pod_name = sanitize_string(job_id, '-')

    job = SparkOperator(
        task_id=job_id,
        name=pod_name,
        arguments=args,
        zone=zone,
        spark_class=main_class,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config="xsmall-etl",
        on_success_callback=Slack.notify_dag_completion,
        dag=dag,
        trigger_rule=TriggerRule.NONE_FAILED
    )

    return job

def get_test_sub_group(zone: str,
                   config_file: str,
                   jar: str,
                   dag: DAG,
                   test_type: str,
                   datasets: list) -> TaskGroup:
    """
    Create task group for tests in a subzone

    :param zone:
    :param config_file:
    :param jar:
    :param dag:
    :param test_type: pre or post
    :param datasets: list of dataset configs
    :return: TaskGoup
    """

    if test_type == "pre":
        @task_group(group_id="pre_tests")
        def pre_tests():
            pre_test_jobs = []

            for conf in datasets:
                dataset_id = conf['dataset_id']
                config_type = conf['cluster_type']

                for pre_test in conf['post_tests']:
                    pre_test_job = create_spark_test(dataset_id, pre_test, zone, config_type, config_file, jar, dag,
                                                     spark_test_failure_msg)
                    pre_test_jobs.append(pre_test_job)

            pre_test_jobs

        return pre_tests()

    elif test_type == "post":
        @task_group(group_id="post_tests")
        def post_tests():
            post_test_jobs = []

            for conf in datasets:
                dataset_id = conf['dataset_id']
                config_type = conf['cluster_type']

                for post_test in conf['post_tests']:
                    post_test_job = create_spark_test(dataset_id, post_test, zone, config_type, config_file, jar, dag,
                                                     spark_test_failure_msg)
                    post_test_jobs.append(post_test_job)

            post_test_jobs

        return post_tests()

    else:
        raise AttributeError(f"Invalid test type: {test_type}. Please provide one of the supported test types: 'pre' or 'post'.")

def setup_dag(dag: DAG,
              dag_config: dict,
              config_file: str,
              jar: str,
              schema: str,
              version: str,
              spark_failure_msg: str,
              skip_task: Optional[str] = None):
    """
    setup a dag
    :param dag:
    :param dag_config:
    :param config_file:
    :param jar:
    :param schema:
    :param version: Version to release, defaults to "latest"
    :param spark_failure_msg:
    :param skip_task: A function to evaluate whether a task should be skipped or not, defaults to None
    :return:
    """

    groups = []
    for step_config in dag_config['steps']:

        with TaskGroup(group_id=step_config['destination_subzone']) as subzone_group:
            zone = step_config['destination_zone']
            subzone = step_config['destination_subzone']
            main_class = step_config['main_class']
            multiple_main_methods = step_config['multiple_main_methods']

            start = notify.start(task_id=f"start_{subzone}_{schema}")
            end = notify.end(f"end_{subzone}_{schema}")
            publish = get_publish_operator(step_config, config_file, jar, dag, spark_failure_msg)

            jobs = {}
            all_dependencies = []
            all_pre_tests = []
            all_post_tests = []
            pre_test_sub_group = None
            post_test_sub_group = None

            for conf in step_config['datasets']:
                all_pre_tests.extend(conf['pre_tests'])
                all_post_tests.extend(conf['post_tests'])

            if len(all_pre_tests) > 0:# generate all pre tests in subzone

                pre_test_sub_group = get_test_sub_group(zone, config_file, jar, dag, "pre", step_config['datasets'])

            if len(all_post_tests) > 0: # generate all post tests in subzone
                post_test_sub_group = get_test_sub_group(zone, config_file, jar, dag, "post", step_config['datasets'])

            for conf in step_config['datasets']:
                dataset_id = conf['dataset_id']
                config_type = conf['cluster_type']
                run_type = conf['run_type']

                job = create_spark_job(dataset_id, zone, subzone, run_type, config_type, config_file, jar, dag, main_class,
                                       multiple_main_methods, version, spark_failure_msg, skip_task)

                all_dependencies.extend(conf['dependencies'])
                jobs[dataset_id] = {"job": job, "dependencies": conf['dependencies']}

            for dataset_id, job in jobs.items():
                for dependency in job['dependencies']:
                    jobs[dependency]['job'] >> job['job']
                if len(job['dependencies']) == 0:
                    if len(all_pre_tests) > 0:
                        pre_test_sub_group >> start >> job['job']
                    else:
                        start >> job['job']
                if dataset_id not in all_dependencies:
                    if len(all_post_tests) > 0:
                        if publish is not None:
                            job['job'] >> end >> post_test_sub_group >> publish
                        else:
                            job['job'] >> end >> post_test_sub_group
                    else:
                        if publish is not None:
                            job['job'] >> end >> publish
                        else:
                            job['job'] >> end

            groups.append(subzone_group)

    # Subzone task groups execution order
    if len(groups) > 1:
        for i in range(0, len(groups) - 1):
            groups[i] >> groups[i+1]
    else:
        groups[0]

def read_json(path: str):
    """
    read json file
    :param path:
    :return:
    """
    return json.load(open(path, encoding='UTF8'))


def get_main_class(subzone: str, main_class: str):
    """
    Return the default main class for the subzone if no main class is provided

    :param subzone: Desination subzone of the task
    :param main_class: main class provided in config file
    :return: main class
    """
    main_classes = {
        "raw": "bio.ferlab.ui.etl.red.raw.Main",
        "curated": "bio.ferlab.ui.etl.red.curated.Main",
        "anonymized": "bio.ferlab.ui.etl.yellow.anonymized.Main",
        "released": "bio.ferlab.ui.etl.green.released.versioned.Main",
        "published": "bio.ferlab.ui.etl.green.published.Main",
    }
    if main_class != "":
        return main_class
    else:
        try:
            return main_classes[subzone]
        except KeyError as err:
            raise KeyError(f"No default main class for subzone: {subzone}. "
                           f"Please provide a main class in config file.") from err


def create_spark_job(destination: str,
                     zone: str,
                     subzone: str,
                     run_type: str,
                     cluster_type: str,
                     config_file: str,
                     jar: str,
                     dag: DAG,
                     main_class: str,
                     multiple_main_methods: bool,
                     version: str,
                     spark_failure_msg: str,
                     skip: Optional[str] = None):
    """
    create spark job operator
    :param destination:
    :param zone:
    :param subzone:
    :param run_type:
    :param cluster_type:
    :param config_file:
    :param jar:
    :param dag:
    :param main_class:
    :param multiple_main_methods: True if the main class contains multiple methods instead of a single run() method
    :param version: Version to release, defaults to "latest"
    :param spark_failure_msg:
    :param skip:
    :return:
    """
    main_class = get_main_class(subzone, main_class)
    args = [config_file, run_type, destination]

    if subzone == "raw":
        if multiple_main_methods:
            args = [
                destination,
                "--config", config_file,
                "--steps", run_type,
                "--app-name", destination
            ]
        else:
            args = [
                "--config", config_file,
                "--steps", run_type,
                "--app-name", destination,
                "--destination", destination
            ]
    elif subzone == "curated" and main_class == "bio.ferlab.ui.etl.red.curated.quanumchartmaxx.Main":
        args = [
                "--config", config_file,
                "--steps", run_type,
                "--app-name", destination,
                "--destination", destination
            ]
    elif subzone == "released":
        args.append(version)

    return SparkOperator(
        task_id=sanitize_string(destination, "_"),
        name=sanitize_string(destination[:40], '-'),
        zone=zone,
        arguments=args,
        spark_class=main_class,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config=f"{cluster_type}-etl",
        dag=dag,
        skip=False if skip is None else skip
    )

def create_spark_test(destination: str,
                      test: str,
                      zone: str,
                      cluster_type: str,
                      config_file: str,
                      jar: str,
                      dag: DAG,
                      spark_failure_msg: str):
    """
    create spark job operator
    :param destination:
    :param zone:
    :param cluster_type:
    :param config_file:
    :param jar:
    :param dag:
    :param spark_failure_msg:
    :return:
    """
    main_class = "bio.ferlab.ui.etl.qa.Main"
    steps = "skip"
    app_name = test + "_" + destination

    args = [
        test,
        "--config", config_file,
        "--steps", steps,
        "--app-name", app_name,
        "--destination", destination
    ]

    task_id = test + "_" + sanitize_string(destination, "_")
    name = test + "-" + sanitize_string(destination[:40], '-')

    return SparkOperator(
        task_id=task_id,
        name=name,
        zone=zone,
        arguments=args,
        spark_class=main_class,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config=f"{cluster_type}-etl",
        dag=dag
    )
