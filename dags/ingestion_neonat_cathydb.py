"""
DAG pour l'ingestion des data de neonat se trouvant dans cathydb
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator

from operators.spark import SparkOperator


NAMESPACE = "raw"
POD_NAME = "raw-ingestion-neonat-cathydb"

MAIN_CLASS = "bio.ferlab.ui.etl.red.raw.neonat.MainNeoCathydb"

JAR = 's3a://spark-prd/jars/unic-etl-{{ params.branch }}.jar'

default_args = {
                'depends_on_past': False,
                'start_date': datetime(2019, 5, 1),
                'provide_context': True  # to use date of ingested data as input in main
                }

dag = DAG(
    dag_id="ingestion_neonat_cathydb",
    start_date=datetime(2019, 5, 1),
    end_date=datetime(2023, 2, 14),
    schedule_interval="@daily",
    params={
        "branch":  Param("master", type="string"),
        "version": Param("latest", type="string")
    },
    dagrun_timeout=timedelta(hours=2),
    default_args=default_args,
    is_paused_upon_creation=True,
    catchup=True
)

with dag:

    start = EmptyOperator("start_ingestion_neonat_cathydb")

    icca_external_numeric = SparkOperator(
        task_id="raw_icca_external_numeric",
        name=POD_NAME,
        arguments=["config/prod.conf", "default", "raw_icca_external_numeric", '{{ds}}'],  # {{ds}} input date
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_config="medium-etl",
        dag=dag
    )

    icca_piicix_num = SparkOperator(
        task_id="raw_icca_piicix_num",
        name=POD_NAME,
        arguments=["config/prod.conf", "default", "raw_icca_piicix_num", '{{ds}}'],
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_config="medium-etl",
        dag=dag
    )

    icca_external_patient = SparkOperator(
        task_id="raw_icca_external_patient",
        name=POD_NAME,
        arguments=["config/prod.conf", "default", "raw_icca_external_patient", '{{ds}}'],
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_config="medium-etl",
        dag=dag)

    icca_external_wave = SparkOperator(
        task_id="raw_icca_external_wave",
        name=POD_NAME,
        arguments=["config/prod.conf", "default", "raw_icca_external_wave", '{{ds}}'],
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_config="medium-etl",
        dag=dag
    )

    icca_piicix_sig = SparkOperator(
        task_id="raw_icca_piicix_sig",
        name=POD_NAME,
        arguments=["config/prod.conf", "default", "raw_icca_piicix_sig", '{{ds}}'],
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_config="medium-etl",
        dag=dag
    )

    icca_piicix_sig_calibre = SparkOperator(
        task_id="raw_icca_piicix_sig_calibre",
        name=POD_NAME,
        arguments=["config/prod.conf", "default", "raw_icca_piicix_sig_calibre", '{{ds}}'],
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_config="medium-etl",
        dag=dag
    )

    icca_piicix_alertes = SparkOperator(
        task_id="raw_icca_piicix_alertes",
        name=POD_NAME,
        arguments=["config/prod.conf", "default", "raw_icca_piicix_alertes", '{{ds}}'],
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_config="medium-etl",
        dag=dag
    )

    end = EmptyOperator("publish_ingestion_neonat_cathydb")

    start >> icca_external_patient >> [icca_piicix_num, icca_piicix_sig, icca_piicix_sig_calibre, icca_piicix_alertes] >> end
    start >> [icca_external_numeric, icca_external_wave, icca_external_patient] >> end
