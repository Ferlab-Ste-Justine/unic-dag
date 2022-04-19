import json
from datetime import datetime

import yaml
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor


def update_log_table(schemas: list,
                     log_table: str,
                     config_file: str,
                     main_class: str,
                     dag: DAG):
    task = f"log_update_{'_'.join(schemas)}_ingestion_log"
    pod_name = task.replace("_", "-")
    yml = log_job("ingestion", pod_name, log_table, "set", schemas, config_file, main_class)
    create_job = SparkKubernetesOperator(
        task_id=task,
        namespace="ingestion",
        application_file=yml,
        priority_weight=1,
        weight_rule="absolute",
        do_xcom_push=True,
        dag=dag
    )
    check_job = SparkKubernetesSensor(
        task_id=f'check_{task}',
        namespace="ingestion",
        priority_weight=999,
        weight_rule="absolute",
        application_name=f"{{{{ task_instance.xcom_pull(task_ids='{task}')['metadata']['name'] }}}}",
        poke_interval=30,
        timeout=21600,  # 6 hours
        dag=dag,
    )
    create_job >> check_job
    return create_job


def setup_dag(dag: DAG,
              config: dict,
              namespace: str,
              config_file: str):
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
                                   dag)
    else:
        publish = DummyOperator(
            task_id="publish_operator",
            dag=dag
        )

    jobs = {}

    for conf in config['datasets']:
        dataset_id = conf['dataset_id']
        create_job = create_spark_job(dataset_id, namespace, conf['run_type'], conf['cluster_type'], config_file, dag,
                                      config['main_class'])
        check_job = check_spark_job(dataset_id, namespace, dag)

        create_job >> check_job >> publish
        jobs[dataset_id] = {"create_job": create_job, "check_job": check_job, "dependencies": conf['dependencies']}

    for j in jobs:
        for dependency in jobs[j]['dependencies']:
            jobs[dependency]['check_job'] >> jobs[j]['create_job']
        if len(jobs[j]['dependencies']) == 0:
            start >> jobs[j]['create_job']


def read_json(path: str):
    f = open(path)
    return json.load(f)


def create_spark_job(destination: str,
                     namespace: str,
                     run_type: str,
                     cluster_type: str,
                     config_file: str,
                     dag: DAG,
                     main_class: str):
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

    pod_name = destination[:40].replace("_", "-")
    yml = ingestion_job(namespace, pod_name, destination, run_type, config_file, main_class, driver_ram, driver_core,
                        worker_ram, worker_core, worker_number)
    if namespace == "anonymized":
        yml = anonymized_job(namespace, pod_name, destination, run_type, config_file, main_class, driver_ram,
                             driver_core, worker_ram, worker_core, worker_number)
    if namespace == "curated":
        yml = curated_job(namespace, pod_name, destination, run_type, config_file, main_class, driver_ram, driver_core,
                          worker_ram, worker_core, worker_number)
    if namespace == "enriched":
        yml = enriched_job(namespace, pod_name, destination, run_type, config_file, main_class, driver_ram, driver_core,
                           worker_ram, worker_core, worker_number)
    if namespace == "warehouse":
        yml = warehouse_job(namespace, pod_name, destination, run_type, config_file, main_class, driver_ram, driver_core,
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


def generic_job(namespace: str,
                pod_name: str,
                arguments: list,
                jar: str,
                main_class: str,
                env: dict,
                driver_ram: int = 32,
                driver_core: int = 8,
                worker_ram: int = 32,
                worker_core: int = 8,
                worker_number: int = 1,
                dependencies: dict = DEPENDENCIES,
                spark_conf: dict = SPARK_CONF,
                spark_version: str = "3.0.0",
                image: str = "ferlabcrsj/spark-operator:3.0.0",
                service_account: str = "spark"):
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
                   main_class: str = "bio.ferlab.ui.etl.yellow.anonymized.Main",
                   driver_ram: int = 40,
                   driver_core: int = 8,
                   worker_ram: int = 40,
                   worker_core: int = 8,
                   worker_number: int = 1):
    return generic_job(namespace,
                       pod_name,
                       [conf, run_type, destination],
                       "s3a://spark-prd/jars/unic-etl-3.0.0.jar",
                       main_class,
                       ANONYMIZED_ENV,
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
                main_class: str = "bio.ferlab.ui.etl.yellow.anonymized.Main",
                driver_ram: int = 40,
                driver_core: int = 8,
                worker_ram: int = 40,
                worker_core: int = 8,
                worker_number: int = 1):
    return generic_job(namespace,
                       pod_name,
                       [conf, run_type, destination],
                       "s3a://spark-prd/jars/unic-etl-3.0.0.jar",
                       main_class,
                       CURATED_ENV,
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
                 main_class: str,
                 driver_ram: int = 40,
                 driver_core: int = 8,
                 worker_ram: int = 40,
                 worker_core: int = 8,
                 worker_number: int = 1):
    return generic_job(namespace,
                       pod_name,
                       [conf, run_type, destination],
                       "s3a://spark-prd/jars/unic-etl-3.0.0.jar",
                       main_class,
                       ENRICHED_ENV,
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
                  main_class: str,
                  driver_ram: int = 40,
                  driver_core: int = 8,
                  worker_ram: int = 40,
                  worker_core: int = 8,
                  worker_number: int = 1):
    return generic_job(namespace,
                       pod_name,
                       [conf, run_type, destination],
                       "s3a://spark-prd/jars/unic-etl-3.0.0.jar",
                       main_class,
                       WAREHOUSE_ENV,
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
                  main_class: str = "bio.ferlab.ui.etl.red.raw.Main",
                  driver_ram: int = 40,
                  driver_core: int = 8,
                  worker_ram: int = 40,
                  worker_core: int = 8,
                  worker_number: int = 1):
    return generic_job(namespace,
                       pod_name,
                       [conf, run_type, destination],
                       "s3a://spark-prd/jars/unic-etl-3.0.0.jar",
                       main_class,
                       INGESTION_ENV,
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
            main_class: str):
    return generic_job(namespace,
                       pod_name,
                       [conf, log_table, run_type] + schemas,
                       "s3a://spark-prd/jars/unic-etl-3.0.0.jar",
                       main_class,
                       INGESTION_ENV,
                       16,
                       4,
                       4,
                       1,
                       1)
