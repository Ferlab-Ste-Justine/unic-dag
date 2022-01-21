import json
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.utils.dates import days_ago

now = datetime.now()
dt_string = now.strftime("%d%m%Y-%H%M%S")


def read_json(path: str):
    f = open(path)
    return json.load(f)


def ingestion_dag(dagid: str,
                  namespace: str,
                  schema: str,
                  config_file: str,
                  args: dict):
    dag = DAG(
        dag_id=dagid,
        schedule_interval=None,
        default_args=args,
        start_date=days_ago(2),
        concurrency=1,
        catchup=False
    )
    with dag:
        start = DummyOperator(
            task_id="start_operator",
            dag=dag
        )
    return dag


def create_spark_job(destination: str,
                     namespace: str,
                     run_type: str,
                     config_file: str,
                     dag: DAG):
    yml = ingestion_job(namespace, destination, run_type, config_file)
    if namespace == "anonymized":
        yml = anonymized_job(namespace, destination, run_type, config_file)

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


def ingestion_job(namespace: str,
                  destination: str,
                  run_type: str,
                  conf: str,
                  main_class: str = "bio.ferlab.ui.etl.red.raw.Main",
                  driver_ram: int = 32,
                  driver_core: int = 8,
                  worker_ram: int = 32,
                  worker_core: int = 8,
                  worker_number: int = 1):
    yml = f"""
    apiVersion: "sparkoperator.k8s.io/v1beta2"
    kind: SparkApplication
    metadata:
      name: {destination[:40].replace("_", "-")}-{dt_string}
      namespace: {namespace}
    spec:
      type: Scala
      mode: cluster
      image: ferlabcrsj/spark-operator:3.0.0
      imagePullPolicy: Always
      deps:
        repositories:
          - https://repos.spark-packages.org
        packages:
          - io.delta:delta-core_2.12:0.8.0
          - org.postgresql:postgresql:42.2.23
          - com.microsoft.azure:spark-mssql-connector_2.12:1.1.0
          - com.microsoft.aad:adal4j:0.0.2
          - com.microsoft.sqlserver:mssql-jdbc:8.4.1.jre8
      mainClass: {main_class}
      mainApplicationFile: "s3a://spark-prd/jars/unic-etl-3.0.0.jar"
      arguments:
        - "{conf}"
        - "{run_type}"
        - "{destination}"
      sparkVersion: "3.0.0"
      sparkConf:
        spark.sql.legacy.timeParserPolicy: "CORRECTED"
        spark.sql.legacy.parquet.datetimeRebaseModeInWrite: "CORRECTED"
        spark.hadoop.fs.s3a.endpoint: "https://minio-unic.infojutras.com"
        spark.hadoop.fs.s3a.impl: "org.apache.hadoop.fs.s3a.S3AFileSystem"
        spark.hadoop.fs.s3a.aws.credentials.provider: "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
        spark.hadoop.fs.s3a.path.style.access: "true"
        spark.hadoop.fs.s3a.connection.ssl.enabled: "true"
        extraJavaOptions: "-Dcom.amazonaws.services.s3.enableV4=true"
        spark.driver.extraJavaOptions: "-Divy.cache.dir=/tmp -Divy.home=/tmp"
      restartPolicy:
        type: Never
      driver:
        cores: {driver_core}
        memory: "{driver_ram}G"
        labels:
          version: 3.0.0
        serviceAccount: spark
        envSecretKeyRefs:
          AWS_ACCESS_KEY_ID:
            name: spark-ingestion-minio
            key: AWS_ACCESS_KEY_ID
          AWS_SECRET_ACCESS_KEY:
            name: spark-ingestion-minio
            key: AWS_SECRET_ACCESS_KEY
          ICCA_DB_USERNAME:
            name: spark-ingestion-icca-db
            key: ICCA_DB_USERNAME
          ICCA_DB_PASSWORD:
            name: spark-ingestion-icca-db
            key: ICCA_DB_PASSWORD
          INTEGRATION_DB_USERNAME:
            name: spark-ingestion-integration-db
            key: INTEGRATION_DB_USERNAME
          INTEGRATION_DB_PASSWORD:
            name: spark-ingestion-integration-db
            key: INTEGRATION_DB_PASSWORD
    
      executor:
        cores: {worker_core}
        instances: {worker_number}
        memory: "{worker_ram}G"
        labels:
          version: 3.0.0
        envSecretKeyRefs:
          AWS_ACCESS_KEY_ID:
            name: spark-ingestion-minio
            key: AWS_ACCESS_KEY_ID
          AWS_SECRET_ACCESS_KEY:
            name: spark-ingestion-minio
            key: AWS_SECRET_ACCESS_KEY
          ICCA_DB_USERNAME:
            name: spark-ingestion-icca-db
            key: ICCA_DB_USERNAME
          ICCA_DB_PASSWORD:
            name: spark-ingestion-icca-db
            key: ICCA_DB_PASSWORD
          INTEGRATION_DB_USERNAME:
            name: spark-ingestion-integration-db
            key: INTEGRATION_DB_USERNAME
          INTEGRATION_DB_PASSWORD:
            name: spark-ingestion-integration-db
            key: INTEGRATION_DB_PASSWORD
    """
    return yml


def anonymized_job(namespace: str,
                   destination: str,
                   run_type: str,
                   conf: str,
                   main_class: str = "bio.ferlab.ui.etl.yellow.anonymized.Main",
                   driver_ram: int = 32,
                   driver_core: int = 8,
                   worker_ram: int = 32,
                   worker_core: int = 8,
                   worker_number: int = 1):
    yml = f"""
    apiVersion: "sparkoperator.k8s.io/v1beta2"
    kind: SparkApplication
    metadata:
      name: {destination[:40].replace("_", "-")}-{dt_string}
      namespace: {namespace}
    spec:
      type: Scala
      mode: cluster
      image: ferlabcrsj/spark-operator:3.0.0
      imagePullPolicy: Always
      deps:
        repositories:
          - https://repos.spark-packages.org
        packages:
          - io.delta:delta-core_2.12:0.8.0
          - org.postgresql:postgresql:42.2.23
          - com.microsoft.azure:spark-mssql-connector_2.12:1.1.0
          - com.microsoft.aad:adal4j:0.0.2
          - com.microsoft.sqlserver:mssql-jdbc:8.4.1.jre8
      mainClass: {main_class}
      mainApplicationFile: "s3a://spark-prd/jars/unic-etl-3.0.0.jar"
      arguments:
        - "{conf}"
        - "{run_type}"
        - "{destination}"
      sparkVersion: "3.0.0"
      sparkConf:
        spark.sql.legacy.timeParserPolicy: "CORRECTED"
        spark.sql.legacy.parquet.datetimeRebaseModeInWrite: "CORRECTED"
        spark.hadoop.fs.s3a.endpoint: "https://minio-unic.infojutras.com"
        spark.hadoop.fs.s3a.impl: "org.apache.hadoop.fs.s3a.S3AFileSystem"
        spark.hadoop.fs.s3a.aws.credentials.provider: "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
        spark.hadoop.fs.s3a.path.style.access: "true"
        spark.hadoop.fs.s3a.connection.ssl.enabled: "true"
        extraJavaOptions: "-Dcom.amazonaws.services.s3.enableV4=true"
        spark.driver.extraJavaOptions: "-Divy.cache.dir=/tmp -Divy.home=/tmp"
      restartPolicy:
        type: Never
      driver:
        cores: {driver_core}
        memory: "{driver_ram}G"
        labels:
          version: 3.0.0
        serviceAccount: spark
        envSecretKeyRefs:
          AWS_ACCESS_KEY_ID:
            name: spark-anonymized-minio
            key: AWS_ACCESS_KEY_ID
          AWS_SECRET_ACCESS_KEY:
            name: spark-anonymized-minio
            key: AWS_SECRET_ACCESS_KEY
          ANONYMIZED_SALT:
            name: spark-anonymized-salt
            key: ANONYMIZED_SALT
    
      executor:
        cores: {worker_core}
        instances: {worker_number}
        memory: "{worker_ram}G"
        labels:
          version: 3.0.0
        envSecretKeyRefs:
          AWS_ACCESS_KEY_ID:
            name: spark-anonymized-minio
            key: AWS_ACCESS_KEY_ID
          AWS_SECRET_ACCESS_KEY:
            name: spark-anonymized-minio
            key: AWS_SECRET_ACCESS_KEY
          ANONYMIZED_SALT:
            name: spark-anonymized-salt
            key: ANONYMIZED_SALT
        """
    return yml
