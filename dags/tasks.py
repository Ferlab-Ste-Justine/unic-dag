"""
Create tasks for a DAG from a JSON configuration file.
"""
# pylint: disable=too-many-statements, too-many-locals
from typing import Optional

from airflow import DAG
from airflow.utils.task_group import TaskGroup

from lib.config import RELEASED_BUCKET, RELEASED_PREFIX, DATE, PUBLISHED_BUCKET, PUBLISHED_PREFIX, UNDERSCORE_DATE, \
    GREEN_MINIO_CONN_ID, UNDERSCORE_VERSION, VERSION
from lib.groups.qa import tests as qa_group
from lib.operators.spark import SparkOperator
from lib.tasks import notify
from lib.tasks.excel import parquet_to_excel
from lib.tasks.optimize import optimize
from lib.utils import sanitize_string, extract_table_name


def _get_version(pass_date: bool, underscore: bool = False):
    """
    Get the version to release. If the version is 'latest', it means no version was provided so we can check if
    pass_date is True. If it's True, we pass the interval end date as a version. Otherwise, we pass the provided version.

    :param pass_date: True to pass the interval end date as a version
    :param underscore: True if the version should be formatted with underscores instead of dashes
    :return: Jinja-templated string to get the version
    """
    if underscore:
        date = UNDERSCORE_DATE
        version = UNDERSCORE_VERSION
    else:
        date = DATE
        version = VERSION
    return f"{{% if params.version == 'latest' and {pass_date} %}}{date}" \
           f"{{% else %}}{version}{{% endif %}}"


def _get_main_class(subzone: str, main_class: str):
    """
    Get the main class to use for the Spark task. If no main class is provided in the config file, we use the default.

    :param subzone: Destination subzone of the task
    :param main_class: Main class provided in the config file
    :return: Main class to use for the Spark task
    """
    main_classes = {
        "raw": "bio.ferlab.ui.etl.red.raw.Main",
        "curated": "bio.ferlab.ui.etl.red.curated.Main",
        "anonymized": "bio.ferlab.ui.etl.yellow.anonymized.Main",
        "released": "bio.ferlab.ui.etl.green.released.Main",
        "published": "bio.ferlab.ui.etl.green.published.Main",
    }
    if main_class != "":
        return main_class

    try:
        return main_classes[subzone]
    except KeyError as err:
        raise KeyError(f"No default main class for subzone: {subzone}. "
                       f"Please provide a main class in config file.") from err


def _create_spark_task(destination: str,
                       zone: str,
                       subzone: str,
                       run_type: str,
                       pass_date: bool,
                       cluster_type: str,
                       config_file: str,
                       jar: str,
                       dag: DAG,
                       main_class: str,
                       multiple_main_methods: bool,
                       spark_failure_msg: str,
                       skip: Optional[str] = None):
    """
    Create a SparkOperator task.

    :param destination: Dataset ID of the destination
    :param zone: Destination zone (i.e. red, yellow, green)
    :param subzone: Destination subzone (i.e. raw, curated, anonymized, released, published)
    :param run_type: Type of run (i.e. default, initial)
    :param pass_date: True to pass --date arg to the main class. The date passed is the end date of the DAG run.
    :param cluster_type: Cluster type (i.e. small-etl, medium-etl, large-etl)
    :param config_file: Path to the ETL config file
    :param jar: Path to the ETL jar file
    :param dag: DAG object
    :param main_class: Main class to use for the Spark task
    :param multiple_main_methods: True if the main class contains multiple methods instead of a single run() method
    :param spark_failure_msg: Message to display in case of Spark job failure
    :param skip: A function to evaluate whether a task should be skipped or not, defaults to None
    :return: SparkOperator task
    """
    main_class = _get_main_class(subzone, main_class)
    version = _get_version(pass_date)
    args = [config_file, run_type, destination]

    if subzone in ["raw", "curated", "released", "enriched"]:
        args = [
            "--config", config_file,
            "--steps", run_type,
            "--app-name", destination
        ]

        if multiple_main_methods:
            args = [destination] + args

        if subzone == "enriched" and pass_date:
            args = args + ["--date", DATE]

        # There are no Mains that need destination arg in enriched
        # If there are multiple main methods, we don't need to pass a destination arg
        elif subzone != "enriched" and not multiple_main_methods:
            args = args + ["--destination", destination]

    if subzone in ["released"]:
        args = args + ["--version", version]

    elif subzone in ["published"]:
        args = args + [version]

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


def _create_publish_excel_task(destination: str,
                               resource: str,
                               pass_date: bool,
                               skip: Optional[str] = None):
    """
    Create a Python task to convert Parquet files in released to Excel files in published.

    :param destination: Dataset ID of the destination to publish
    :param resource: Resource name
    :param pass_date: True if the date should be passed as version. If False, the version provided to the DAG is passed.
    :param skip: A function to evaluate whether a task should be skipped or not, defaults to None
    :return: ParquetToExcel Python task
    """
    table_name = extract_table_name(destination, resource)
    version = _get_version(pass_date, underscore=False)
    underscore_version = _get_version(pass_date, underscore=True)
    return parquet_to_excel.override(task_id=sanitize_string(destination, "_"))(
        parquet_bucket_name=RELEASED_BUCKET,
        parquet_dir_key=f"{RELEASED_PREFIX}/{resource}/{version}/{table_name}",
        excel_bucket_name=PUBLISHED_BUCKET,
        excel_output_key=f"{PUBLISHED_PREFIX}/{resource}/{version}/{resource}_{table_name}_{underscore_version}.xlsx",
        minio_conn_id=GREEN_MINIO_CONN_ID,
        skip=False if skip is None else skip
    )


def create_tasks(dag: DAG,
                 dag_config: dict,
                 config_file: str,
                 jar: str,
                 resource: str,
                 spark_failure_msg: str,
                 skip_task: Optional[str] = None):
    """
    Create tasks for a DAG.
    :param dag: DAG object
    :param dag_config: DAG configuration file
    :param config_file: Path to the ETL config file
    :param jar: Path to the ETL jar file
    :param resource: Resource code (i.e. opera, philips, etc.)
    :param spark_failure_msg: Message to display in case of Spark job failure
    :param skip_task: A function to evaluate whether a task should be skipped or not, defaults to None
    """
    groups = []
    for step_config in dag_config['steps']:
        with TaskGroup(group_id=step_config['destination_subzone']) as subzone_group:
            zone = step_config['destination_zone']
            subzone = step_config['destination_subzone']
            main_class = step_config['main_class']
            multiple_main_methods = step_config['multiple_main_methods']

            start = notify.start(task_id=f"start_{subzone}_{resource}")
            end = notify.end(f"end_{subzone}_{resource}")

            jobs = {}
            all_dependencies = []
            optimize_tables = step_config['optimize']
            pre_tests = step_config['pre_tests']
            post_tests = step_config['post_tests']
            pre_test_sub_group = None
            post_test_sub_group = None

            if pre_tests:
                pre_test_sub_group = qa_group.tests.override(group_id="pre_tests")(
                    pre_tests, resource, zone, subzone, config_file, jar, dag)

            if post_tests:
                post_test_sub_group = qa_group.tests.override(group_id="post_tests")(
                    post_tests, resource, zone, subzone, config_file, jar, dag)

            if optimize_tables:
                optimize_task = optimize(optimize_tables, resource, zone, subzone, config_file, jar, dag)

            for conf in step_config['datasets']:
                dataset_id = conf['dataset_id']
                config_type = conf['cluster_type']
                run_type = conf['run_type']
                pass_date = conf['pass_date']

                # When no main class is defined for published tasks, we create a Python task to publish Excel files
                if subzone == "published" and not main_class:
                    job = _create_publish_excel_task(dataset_id, resource, pass_date, skip_task)

                # For all other tasks, we create a SparkOperator task
                else:
                    job = _create_spark_task(dataset_id, zone, subzone, run_type, pass_date, config_type, config_file,
                                             jar, dag, main_class, multiple_main_methods, spark_failure_msg, skip_task)

                all_dependencies.extend(conf['dependencies'])
                jobs[dataset_id] = {"job": job, "dependencies": conf['dependencies']}

            for dataset_id, job in jobs.items():
                for dependency in job['dependencies']:
                    jobs[dependency]['job'] >> job['job']
                if len(job['dependencies']) == 0:
                    if pre_tests:
                        pre_test_sub_group >> start >> job['job']
                    else:
                        start >> job['job']
                if dataset_id not in all_dependencies:
                    if post_tests and optimize_tables:
                        job['job'] >> optimize_task >> end >> post_test_sub_group
                    elif not post_tests and optimize_tables:
                        job['job'] >> optimize_task >> end
                    elif post_tests and not optimize_tables:
                        job['job'] >> end >> post_test_sub_group
                    else:
                        job['job'] >> end

            groups.append(subzone_group)

    # Subzone task groups execution order
    if len(groups) > 1:
        for i in range(0, len(groups) - 1):
            groups[i] >> groups[i + 1]
    else:
        groups[0]
