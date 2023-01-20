"""
Help class containing custom SparkKubernetesOperator
"""
import json
import re
from datetime import datetime

import yaml
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor


def sanitize_string(string: str, replace_by: str):
    """
    Replace all special character in a string into another character
    :param string: string to be sanitized
    :param replace_by: replacement character
    :return: sanitized string
    """
    return re.sub("[^a-zA-Z0-9 ]", replace_by, string)


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
                         config_file: str,
                         jar: str,
                         dag: DAG,
                         image: str,
                         schema: str):
    """
    Create a publish task based on the config publish_class

    :param dag_config:
    :param config_file:
    :param jar:
    :param dag:
    :param image: The spark-operator Docker image
    :param schema:
    :return: both start and end to the publish operator if the operator contain multiple task
    """
    namespace = dag_config['namespace']
    schemas = dag_config['schemas']
    main_class = dag_config['publish_class']
    cluster_specs = get_cluster_specs("xsmall")

    if main_class == "bio.ferlab.ui.etl.red.raw.UpdateLog":
        create_job_id = f"log_update_{'_'.join(schemas)[:20].lower()}"
        args = [config_file, "journalisation.ETL_Truncate_Table", "set", *schemas]

    elif main_class == "bio.ferlab.ui.etl.green.published.coda.PublishToAidbox":
        create_job_id = "publish_to_aidbox"
        args = [config_file, *schemas]

    else:
        publish = DummyOperator(
            task_id=f"publish_{dag_config['namespace']}_{schema}",
            dag=dag
        )
        return publish, publish

    pod_name = sanitize_string(create_job_id, '-')
    yml = generic_job(namespace, pod_name, args, jar, main_class, image, **cluster_specs)

    create_job = SparkKubernetesOperator(
        task_id=create_job_id,
        namespace=namespace,
        application_file=yml,
        priority_weight=1,
        weight_rule="absolute",
        do_xcom_push=True,
        dag=dag
    )
    check_job = SparkKubernetesSensor(
        task_id=f'check_{create_job_id}',
        namespace=namespace,
        priority_weight=999,
        weight_rule="absolute",
        application_name=f"{{{{ task_instance.xcom_pull(task_ids='{create_job_id}')['metadata']['name'] }}}}",
        poke_interval=30,
        timeout=21600,  # 6 hours
        dag=dag,
    )
    create_job >> check_job
    return create_job, check_job


def setup_dag(dag: DAG,
              dag_config: dict,
              config_file: str,
              jar: str,
              image: str,
              schema: str,
              version: str):
    """
    setup a dag
    :param dag:
    :param dag_config:
    :param config_file:
    :param jar:
    :param image: The spark-operator Docker image
    :param schema:
    :param version: Version to release, defaults to "latest"
    :return:
    """

    previous_publish = None

    for step_config in dag_config['steps']:
        start = get_start_operator(step_config['namespace'], dag, schema)
        start_publish, end_publish = get_publish_operator(step_config, config_file, jar, dag, image, schema)

        if previous_publish:
            previous_publish >> start
        previous_publish = end_publish

        jobs = {}
        all_dependencies = []

        for conf in step_config['datasets']:
            dataset_id = conf['dataset_id']

            create_job = create_spark_job(dataset_id, step_config['namespace'], conf['run_type'], conf['cluster_type'],
                                          conf['cluster_specs'], config_file, jar, image, dag,
                                          step_config['main_class'], version)
            check_job = check_spark_job(dataset_id, step_config['namespace'], dag)

            create_job >> check_job
            all_dependencies = all_dependencies + conf['dependencies']
            jobs[dataset_id] = {"create_job": create_job, "check_job": check_job, "dependencies": conf['dependencies']}

        for dataset_id, job in jobs.items():
            for dependency in job['dependencies']:
                jobs[dependency]['check_job'] >> job['create_job']
            if len(job['dependencies']) == 0:
                start >> job['create_job']
            if dataset_id not in all_dependencies:
                job['check_job'] >> start_publish


def read_json(path: str):
    """
    read json file
    :param path:
    :return:
    """
    return json.load(open(path, encoding='UTF8'))


def get_cluster_specs(cluster_type: str, cluster_specs: dict = None):
    """
    Return cluster specs based on cluster_type

    :param cluster_type: string representing the cluster size: xsmall, small, medium or large
    :param cluster_specs: specs to optionally override the default cluster_type ones
    :return: a dict with the cluster specs
    """
    driver_ram = "driver_ram"
    driver_core = "driver_core"
    worker_number = "worker_number"
    worker_ram = "worker_ram"
    worker_core = "worker_core"

    clusters = {
        "xsmall": {
            driver_ram: 8,
            driver_core: 2,
            worker_ram: 8,
            worker_core: 2,
            worker_number: 1
        },
        "small": {
            driver_ram: 16,
            driver_core: 2,
            worker_ram: 16,
            worker_core: 2,
            worker_number: 1
        },
        "medium": {
            driver_ram: 36,
            driver_core: 6,
            worker_ram: 36,
            worker_core: 6,
            worker_number: 2
        },
        "large": {
            driver_ram: 40,
            driver_core: 8,
            worker_ram: 40,
            worker_core: 8,
            worker_number: 4
        }
    }

    specs = {driver_ram: "", driver_core: "", worker_number: "", worker_ram: "", worker_core: ""}

    for spec in specs.copy():
        specs[spec] = cluster_specs[spec] if cluster_specs is not None and spec in cluster_specs \
            else clusters[cluster_type][spec] if cluster_type in clusters \
            else clusters["xsmall"][spec]

    return specs


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
                     cluster_specs: dict,
                     config_file: str,
                     jar: str,
                     image: str,
                     dag: DAG,
                     main_class: str,
                     version: str):
    """
    create spark job operator
    :param destination:
    :param namespace:
    :param run_type:
    :param cluster_type:
    :param cluster_specs: specs to optionally override the default cluster_type ones
    :param config_file:
    :param jar:
    :param image: The spark-operator Docker image
    :param dag:
    :param main_class:
    :param version: Version to release, defaults to "latest"
    :return:
    """
    pod_name = sanitize_string(destination[:40], '-')
    main_class = get_main_class(namespace, main_class)
    specs = get_cluster_specs(cluster_type, cluster_specs)

    default_args = [config_file, run_type, destination]
    args = default_args.append(version) if namespace == "released" else default_args

    yml = generic_job(namespace, pod_name, args, jar, main_class, image, **specs)

    return SparkKubernetesOperator(
        task_id=sanitize_string(f"create_{destination}", "_"),
        namespace=namespace,
        application_file=yml,
        priority_weight=1,
        weight_rule="absolute",
        do_xcom_push=True,
        dag=dag
    )


def check_spark_job(destination: str,
                    namespace: str,
                    dag: DAG):
    """
    check spark job sensor
    :param destination:
    :param namespace:
    :param dag:
    :return:
    """
    task_to_check = sanitize_string(f"create_{destination}", "_")
    return SparkKubernetesSensor(
        task_id=sanitize_string(f"check_{destination}", "_"),
        namespace=namespace,
        priority_weight=999,
        weight_rule="absolute",
        application_name=f"{{{{ task_instance.xcom_pull(task_ids='{task_to_check}')['metadata']['name'] }}}}",
        poke_interval=30,
        timeout=21600,  # 6 hours
        dag=dag,
    )


DEPENDENCIES = {
    "repositories": ["https://repos.spark-packages.org"],
    "packages": ["io.delta:delta-core_2.12:0.8.0",
                 "org.postgresql:postgresql:42.2.23",
                 "com.microsoft.azure:spark-mssql-connector_2.12:1.1.0",
                 "com.microsoft.aad:adal4j:0.0.2",
                 "com.microsoft.sqlserver:mssql-jdbc:8.4.1.jre8"]
}

SPARK_CONF = {
    "spark.sql.legacy.timeParserPolicy": "CORRECTED",
    "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "CORRECTED",
    "spark.hadoop.fs.s3a.endpoint": "https://minio.unic.ferlab.bio",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
    "extraJavaOptions": "-Dcom.amazonaws.services.s3.enableV4=true",
    "spark.driver.extraJavaOptions": "-Divy.cache.dir=/tmp -Divy.home=/tmp"
}

ENV_VARIABLES = {
    "ingestion": {
        "AWS_ACCESS_KEY_ID": {
            "name": "spark-ingestion-minio",
            "key": "AWS_ACCESS_KEY_ID"
        },
        "AWS_SECRET_ACCESS_KEY": {
            "name": "spark-ingestion-minio",
            "key": "AWS_SECRET_ACCESS_KEY"
        },
        "ICCA_DB_USERNAME": {
            "name": "spark-ingestion-icca-db",
            "key": "ICCA_DB_USERNAME"
        },
        "ICCA_DB_PASSWORD": {
            "name": "spark-ingestion-icca-db",
            "key": "ICCA_DB_PASSWORD"
        },
        "INTEGRATION_DB_USERNAME": {
            "name": "spark-ingestion-integration-db",
            "key": "INTEGRATION_DB_USERNAME"
        },
        "INTEGRATION_DB_PASSWORD": {
            "name": "spark-ingestion-integration-db",
            "key": "INTEGRATION_DB_PASSWORD"
        }
    },
    "curated": {
        "AWS_ACCESS_KEY_ID": {
            "name": "spark-curated-minio",
            "key": "AWS_ACCESS_KEY_ID"
        },
        "AWS_SECRET_ACCESS_KEY": {
            "name": "spark-curated-minio",
            "key": "AWS_SECRET_ACCESS_KEY"
        }
    },
    "anonymized": {
        "AWS_ACCESS_KEY_ID": {
            "name": "spark-anonymized-minio",
            "key": "AWS_ACCESS_KEY_ID"
        },
        "AWS_SECRET_ACCESS_KEY": {
            "name": "spark-anonymized-minio",
            "key": "AWS_SECRET_ACCESS_KEY"
        },
        "ANONYMIZED_SALT": {
            "name": "spark-anonymized-salt",
            "key": "ANONYMIZED_SALT"
        }
    },
    "warehouse": {
        "AWS_ACCESS_KEY_ID": {
            "name": "spark-warehouse-minio",
            "key": "AWS_ACCESS_KEY_ID"
        },
        "AWS_SECRET_ACCESS_KEY": {
            "name": "spark-warehouse-minio",
            "key": "AWS_SECRET_ACCESS_KEY"
        }
    },
    "enriched": {
        "AWS_ACCESS_KEY_ID": {
            "name": "spark-enriched-minio",
            "key": "AWS_ACCESS_KEY_ID"
        },
        "AWS_SECRET_ACCESS_KEY": {
            "name": "spark-enriched-minio",
            "key": "AWS_SECRET_ACCESS_KEY"
        }
    },
    "released": {
        "AWS_ACCESS_KEY_ID": {
            "name": "spark-released-minio",
            "key": "AWS_ACCESS_KEY_ID"
        },
        "AWS_SECRET_ACCESS_KEY": {
            "name": "spark-released-minio",
            "key": "AWS_SECRET_ACCESS_KEY"
        }
    },
    "published": {
        "AWS_ACCESS_KEY_ID": {
            "name": "spark-published-minio",
            "key": "AWS_ACCESS_KEY_ID"
        },
        "AWS_SECRET_ACCESS_KEY": {
            "name": "spark-published-minio",
            "key": "AWS_SECRET_ACCESS_KEY"
        },
        "CODA_AIDBOX_USERNAME": {
            "name": "coda-aidbox",
            "key": "CODA_AIDBOX_USERNAME"
        },
        "CODA_AIDBOX_PASSWORD": {
            "name": "coda-aidbox",
            "key": "CODA_AIDBOX_PASSWORD"
        }
    }
}


# pylint: disable=too-many-locals
def generic_job(namespace: str,
                pod_name: str,
                arguments: list,
                jar: str,
                main_class: str,
                image: str,
                driver_ram: int = 32,
                driver_core: int = 8,
                worker_ram: int = 32,
                worker_core: int = 8,
                worker_number: int = 1,
                env_variables: dict = None,
                dependencies: dict = None,
                spark_conf: dict = None,
                spark_version: str = "3.0.0",
                service_account: str = "spark"):
    """
    Generic yml representing spark job

    :param namespace: Kubernetes namespace
    :param pod_name: Kubernetes pod name
    :param arguments:
    :param jar: location of the jar containing the ETL jobs
    :param main_class: ETL main class
    :param image: SparkOperator Docker image
    :param driver_ram: RAM for the driver pod
    :param driver_core: number of cores for the driver pod
    :param worker_ram: RAM for the worker pods
    :param worker_core: number of cores for the worker pods
    :param worker_number: number of worker pods
    :param env_variables: environment variables
    :param dependencies:
    :param spark_conf: ETL config file
    :param spark_version: spark version
    :param service_account: Kubernetes ServiceAcount
    :return:
    """
    if env_variables is None:
        env_variables = ENV_VARIABLES[namespace]

    if dependencies is None:
        dependencies = DEPENDENCIES

    if spark_conf is None:
        spark_conf = SPARK_CONF

    dt_string = datetime.now().strftime("%d%m%Y-%H%M%S")
    yml = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": f"{pod_name}-{dt_string}",
            "namespace": f"{namespace}"
        },
        "spec": {
            "type": "Scala",
            "mode": "cluster",
            "image": image,
            "imagePullPolicy": "IfNotPresent",
            "deps": dependencies,
            "mainClass": main_class,
            "mainApplicationFile": jar,
            "arguments": arguments,
            "sparkVersion": spark_version,
            "sparkConf": spark_conf,
            "restartPolicy": {
                "type": "OnFailure",
                "onFailureRetries": 1,
                "onFailureRetryInterval": 10,
                "onSubmissionFailureRetries": 1,
                "onSubmissionFailureRetryInterval": 10
            },
            "driver": {
                "cores": driver_core,
                "memory": f"{driver_ram}G",
                "labels": {"version": "3.0.0"},
                "serviceAccount": service_account,
                "envSecretKeyRefs": env_variables,
            },
            "envSecretKeyRefs": env_variables,
            "executor": {
                "cores": worker_core,
                "memory": f"{worker_ram}G",
                "instances": worker_number,
                "labels": {"version": "3.0.0"},
                "serviceAccount": service_account,
                "envSecretKeyRefs": env_variables,
            }
        }
    }
    return yaml.dump(yml)
