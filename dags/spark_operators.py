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


def sanitize_pod_name(name: str):
    """
    Replace all special character in a pod_name into dashes
    :param name: name of the pod before being sanitized
    :return: sanitized name
    """
    return re.sub("[^a-zA-Z0-9 ]", '-', name)


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
    :param dag:
    :return:
    """
    create_job_id = f"log_update_{'_'.join(schemas)[:20]}"
    pod_name = sanitize_pod_name(create_job_id)
    yml = log_job("ingestion", pod_name, log_table, "set", schemas, config_file, main_class, jar)
    create_job = SparkKubernetesOperator(
        task_id=create_job_id,
        namespace="ingestion",
        application_file=yml,
        priority_weight=1,
        weight_rule="absolute",
        do_xcom_push=True,
        dag=dag
    )
    check_job = SparkKubernetesSensor(
        task_id=f'check_{create_job_id}',
        namespace="ingestion",
        priority_weight=999,
        weight_rule="absolute",
        application_name=f"{{{{ task_instance.xcom_pull(task_ids='{create_job_id}')['metadata']['name'] }}}}",
        poke_interval=30,
        timeout=21600,  # 6 hours
        dag=dag,
    )
    create_job >> check_job
    return create_job


def setup_dag(dag: DAG,
              config: dict,
              namespace: str,
              config_file: str,
              jar: str):
    """
    setup a dag
    :param dag:
    :param config:
    :param namespace:
    :param config_file:
    :param jar:
    :return:
    """
    start = DummyOperator(
        task_id="start_operator",
        dag=dag
    )

    publish = None
    if namespace == "ingestion":
        publish = update_log_table(config['schemas'],
                                   "journalisation.ETL_Truncate_Table",
                                   config_file,
                                   config['publish_class'],
                                   jar,
                                   dag)
    else:
        publish = DummyOperator(
            task_id="publish_operator",
            dag=dag
        )

    jobs = {}
    all_dependencies = []

    for conf in config['datasets']:
        dataset_id = conf['dataset_id']

        create_job = create_spark_job(dataset_id, namespace, conf['run_type'], conf['cluster_type'], config_file, jar,
                                      dag, config['main_class'])
        check_job = check_spark_job(dataset_id, namespace, dag)

        create_job >> check_job
        all_dependencies = all_dependencies + conf['dependencies']
        jobs[dataset_id] = {"create_job": create_job, "check_job": check_job, "dependencies": conf['dependencies']}

    for dataset_id, job in jobs.items():
        for dependency in job['dependencies']:
            jobs[dependency]['check_job'] >> job['create_job']
        if len(job['dependencies']) == 0:
            start >> job['create_job']
        if dataset_id not in all_dependencies:
            job['check_job'] >> publish



def read_json(path: str):
    """
    read json file
    :param path:
    :return:
    """
    return json.load(open(path, encoding='UTF8'))


def create_spark_job(destination: str,
                     namespace: str,
                     run_type: str,
                     cluster_type: str,
                     config_file: str,
                     jar: str,
                     dag: DAG,
                     main_class: str):
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
    :return:
    """
    driver_ram = 32
    driver_core = 4
    worker_number = 1
    worker_ram = 32
    worker_core = 4
    if cluster_type == "medium":
        driver_ram = 36
        driver_core = 6
        worker_number = 2
        worker_ram = 36
        worker_core = 6
    if cluster_type == "large":
        driver_ram = 40
        driver_core = 8
        worker_number = 4
        worker_ram = 40
        worker_core = 8

    pod_name = pod_name = sanitize_pod_name(destination[:40])
    yml = ingestion_job(namespace, pod_name, destination, run_type, config_file, jar, main_class, driver_ram, driver_core,
                        worker_ram, worker_core, worker_number)
    if namespace == "anonymized":
        yml = anonymized_job(namespace, pod_name, destination, run_type, config_file, jar, main_class, driver_ram,
                             driver_core, worker_ram, worker_core, worker_number)
    if namespace == "curated":
        yml = curated_job(namespace, pod_name, destination, run_type, config_file, jar, main_class, driver_ram, driver_core,
                          worker_ram, worker_core, worker_number)
    if namespace == "enriched":
        yml = enriched_job(namespace, pod_name, destination, run_type, config_file, jar, main_class, driver_ram, driver_core,
                           worker_ram, worker_core, worker_number)
    if namespace == "warehouse":
        yml = warehouse_job(namespace, pod_name, destination, run_type, config_file, jar, main_class, driver_ram,
                            driver_core,
                            worker_ram, worker_core, worker_number)

    return SparkKubernetesOperator(
        task_id=f"create_{destination}",
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
    return SparkKubernetesSensor(
        task_id=f'check_{destination}',
        namespace=namespace,
        priority_weight=999,
        weight_rule="absolute",
        application_name=f"{{{{ task_instance.xcom_pull(task_ids='create_{destination}')['metadata']['name'] }}}}",
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
    "spark.hadoop.fs.s3a.endpoint": "https://minio-unic.infojutras.com",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
    "extraJavaOptions": "-Dcom.amazonaws.services.s3.enableV4=true",
    "spark.driver.extraJavaOptions": "-Divy.cache.dir=/tmp -Divy.home=/tmp"
}

INGESTION_ENV = {
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
}

CURATED_ENV = {
    "AWS_ACCESS_KEY_ID": {
        "name": "spark-curated-minio",
        "key": "AWS_ACCESS_KEY_ID"
    },
    "AWS_SECRET_ACCESS_KEY": {
        "name": "spark-curated-minio",
        "key": "AWS_SECRET_ACCESS_KEY"
    }
}

ENRICHED_ENV = {
    "AWS_ACCESS_KEY_ID": {
        "name": "spark-enriched-minio",
        "key": "AWS_ACCESS_KEY_ID"
    },
    "AWS_SECRET_ACCESS_KEY": {
        "name": "spark-enriched-minio",
        "key": "AWS_SECRET_ACCESS_KEY"
    }
}

WAREHOUSE_ENV = {
    "AWS_ACCESS_KEY_ID": {
        "name": "spark-warehouse-minio",
        "key": "AWS_ACCESS_KEY_ID"
    },
    "AWS_SECRET_ACCESS_KEY": {
        "name": "spark-warehouse-minio",
        "key": "AWS_SECRET_ACCESS_KEY"
    }
}

ANONYMIZED_ENV = {
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
}

# pylint: disable=too-many-locals
def generic_job(namespace: str,
                pod_name: str,
                arguments: list,
                jar: str,
                main_class: str,
                env: dict,
                dependencies: dict,
                spark_conf: dict,
                driver_ram: int = 32,
                driver_core: int = 8,
                worker_ram: int = 32,
                worker_core: int = 8,
                worker_number: int = 1,
                spark_version: str = "3.0.0",
                image: str = "ferlabcrsj/spark-operator:3.0.0",
                service_account: str = "spark"):
    """
    Generic yml representing spark job
    :param namespace:
    :param pod_name:
    :param arguments:
    :param jar:
    :param main_class:
    :param env:
    :param dependencies:
    :param spark_conf:
    :param driver_ram:
    :param driver_core:
    :param worker_ram:
    :param worker_core:
    :param worker_number:
    :param spark_version:
    :param image:
    :param service_account:
    :return:
    """
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
                "onFailureRetries": 3,
                "onFailureRetryInterval": 10,
                "onSubmissionFailureRetries": 3,
                "onSubmissionFailureRetryInterval": 10
            },
            "driver": {
                "cores": driver_core,
                "memory": f"{driver_ram}G",
                "labels": {"version": "3.0.0"},
                "serviceAccount": service_account,
                "envSecretKeyRefs": env,
            },
            "envSecretKeyRefs": env,
            "executor": {
                "cores": worker_core,
                "memory": f"{worker_ram}G",
                "instances": worker_number,
                "labels": {"version": "3.0.0"},
                "serviceAccount": service_account,
                "envSecretKeyRefs": env,
            }
        }
    }
    return yaml.dump(yml)


def anonymized_job(namespace: str,
                   pod_name: str,
                   destination: str,
                   run_type: str,
                   conf: str,
                   jar: str,
                   main_class: str = "bio.ferlab.ui.etl.yellow.anonymized.Main",
                   driver_ram: int = 40,
                   driver_core: int = 8,
                   worker_ram: int = 40,
                   worker_core: int = 8,
                   worker_number: int = 1):
    """
    yml for anonymized job
    :param namespace:
    :param pod_name:
    :param destination:
    :param run_type:
    :param conf:
    :param jar:
    :param main_class:
    :param driver_ram:
    :param driver_core:
    :param worker_ram:
    :param worker_core:
    :param worker_number:
    :return:
    """
    return generic_job(namespace,
                       pod_name,
                       [conf, run_type, destination],
                       jar,
                       main_class,
                       ANONYMIZED_ENV,
                       DEPENDENCIES,
                       SPARK_CONF,
                       driver_ram,
                       driver_core,
                       worker_ram,
                       worker_core,
                       worker_number)


def curated_job(namespace: str,
                pod_name: str,
                destination: str,
                run_type: str,
                conf: str,
                jar: str,
                main_class: str = "bio.ferlab.ui.etl.yellow.anonymized.Main",
                driver_ram: int = 40,
                driver_core: int = 8,
                worker_ram: int = 40,
                worker_core: int = 8,
                worker_number: int = 1):
    """
    yml for curated job
    :param namespace:
    :param pod_name:
    :param destination:
    :param run_type:
    :param conf:
    :param jar:
    :param main_class:
    :param driver_ram:
    :param driver_core:
    :param worker_ram:
    :param worker_core:
    :param worker_number:
    :return:
    """
    return generic_job(namespace,
                       pod_name,
                       [conf, run_type, destination],
                       jar,
                       main_class,
                       CURATED_ENV,
                       DEPENDENCIES,
                       SPARK_CONF,
                       driver_ram,
                       driver_core,
                       worker_ram,
                       worker_core,
                       worker_number)


def enriched_job(namespace: str,
                 pod_name: str,
                 destination: str,
                 run_type: str,
                 conf: str,
                 jar: str,
                 main_class: str,
                 driver_ram: int = 40,
                 driver_core: int = 8,
                 worker_ram: int = 40,
                 worker_core: int = 8,
                 worker_number: int = 1):
    """
    yml for enriched job
    :param namespace:
    :param pod_name:
    :param destination:
    :param run_type:
    :param conf:
    :param jar:
    :param main_class:
    :param driver_ram:
    :param driver_core:
    :param worker_ram:
    :param worker_core:
    :param worker_number:
    :return:
    """
    return generic_job(namespace,
                       pod_name,
                       [conf, run_type, destination],
                       jar,
                       main_class,
                       ENRICHED_ENV,
                       DEPENDENCIES,
                       SPARK_CONF,
                       driver_ram,
                       driver_core,
                       worker_ram,
                       worker_core,
                       worker_number)


def warehouse_job(namespace: str,
                  pod_name: str,
                  destination: str,
                  run_type: str,
                  conf: str,
                  jar: str,
                  main_class: str,
                  driver_ram: int = 40,
                  driver_core: int = 8,
                  worker_ram: int = 40,
                  worker_core: int = 8,
                  worker_number: int = 1):
    """
    yml for warehouse job
    :param namespace:
    :param pod_name:
    :param destination:
    :param run_type:
    :param conf:
    :param jar:
    :param main_class:
    :param driver_ram:
    :param driver_core:
    :param worker_ram:
    :param worker_core:
    :param worker_number:
    :return:
    """
    return generic_job(namespace,
                       pod_name,
                       [conf, run_type, destination],
                       jar,
                       main_class,
                       WAREHOUSE_ENV,
                       DEPENDENCIES,
                       SPARK_CONF,
                       driver_ram,
                       driver_core,
                       worker_ram,
                       worker_core,
                       worker_number)


def ingestion_job(namespace: str,
                  pod_name: str,
                  destination: str,
                  run_type: str,
                  conf: str,
                  jar: str,
                  main_class: str = "bio.ferlab.ui.etl.red.raw.Main",
                  driver_ram: int = 40,
                  driver_core: int = 8,
                  worker_ram: int = 40,
                  worker_core: int = 8,
                  worker_number: int = 1):
    """
    yml for ingestion job
    :param namespace:
    :param pod_name:
    :param destination:
    :param run_type:
    :param conf:
    :param jar:
    :param main_class:
    :param driver_ram:
    :param driver_core:
    :param worker_ram:
    :param worker_core:
    :param worker_number:
    :return:
    """
    return generic_job(namespace,
                       pod_name,
                       [conf, run_type, destination],
                       jar,
                       main_class,
                       INGESTION_ENV,
                       DEPENDENCIES,
                       SPARK_CONF,
                       driver_ram,
                       driver_core,
                       worker_ram,
                       worker_core,
                       worker_number)


def log_job(namespace: str,
            pod_name: str,
            log_table: str,
            run_type: str,
            schemas: list,
            conf: str,
            jar: str,
            main_class: str):
    """
    yml for ingestion log job
    :param namespace:
    :param pod_name:
    :param log_table:
    :param run_type:
    :param schemas:
    :param conf:
    :param jar:
    :param main_class:
    :return:
    """
    return generic_job(namespace,
                       pod_name,
                       [conf, log_table, run_type] + schemas,
                       jar,
                       main_class,
                       INGESTION_ENV,
                       DEPENDENCIES,
                       SPARK_CONF,
                       16,
                       4,
                       4,
                       1,
                       1)
