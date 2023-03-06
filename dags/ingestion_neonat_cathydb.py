"""
DAG pour l'ingestion des data de neonat se trouvant dans cathydb
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator

from core.slack import Slack
from operators.spark import SparkOperator

NAMESPACE = "raw"
MAIN_CLASS = "bio.ferlab.ui.etl.red.raw.neonat.MainCathyDb"
JAR = 's3a://spark-prd/jars/unic-etl-{{ params.branch }}.jar'

default_args = {
                'depends_on_past': False,
                'start_date': datetime(2020, 5, 24),
                'provide_context': True  # to use date of ingested data as input in main
                }

dag = DAG(
    dag_id="ingestion_neonat_cathydb",
    start_date=datetime(2020, 5, 24),
    end_date=datetime(2023, 3, 6),
    schedule_interval="@daily",
    params={
        "branch":  Param("master", type="string"),
        "version": Param("latest", type="string")
    },
    dagrun_timeout=timedelta(hours=2),
    default_args=default_args,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=2,
    max_active_tasks=3,
    tags=["ingestion"]
)

with dag:

    start = EmptyOperator(
        task_id="start_ingestion_neonat_cathydb",
        on_execute_callback=Slack.notify_dag_start
    )

    # UNCOMMENT EXTERNAL TABLES AFTER TIMESTAMP COLUMN ADDED IN CATHYDB

    # icca_external_numeric = SparkOperator(
    #     task_id="raw_icca_external_numeric",
    #     name="raw-icca-external-numeric",
    #     arguments=["config/prod.conf", "skip", "raw_icca_external_numeric", '{{ds}}'],  # {{ds}} input date
    #     namespace=NAMESPACE,
    #     spark_class=MAIN_CLASS,
    #     spark_jar=JAR,
    #     spark_config="xsmall-etl",
    #     dag=dag
    # )
    #
    # icca_external_patient = SparkOperator(
    #     task_id="raw_icca_external_patient",
    #     name="raw-icca-external-patient",
    #     arguments=["config/prod.conf", "skip", "raw_icca_external_patient", '{{ds}}'],
    #     namespace=NAMESPACE,
    #     spark_class=MAIN_CLASS,
    #     spark_jar=JAR,
    #     spark_config="xsmall-etl",
    #     dag=dag)
    #
    # icca_external_wave = SparkOperator(
    #     task_id="raw_icca_external_wave",
    #     name="raw-icca-external-wave",
    #     arguments=["config/prod.conf", "skip", "raw_icca_external_wave", '{{ds}}'],
    #     namespace=NAMESPACE,
    #     spark_class=MAIN_CLASS,
    #     spark_jar=JAR,
    #     spark_config="xsmall-etl",
    #     dag=dag
    # )

    icca_piicix_num = SparkOperator(
        task_id="raw_icca_piicix_num",
        name="raw-icca-piicix-num",
        arguments=["config/prod.conf", "default", "raw_icca_piicix_num", '{{ds}}'],
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_config="medium-etl",
        dag=dag
    )

    icca_piicix_sig = SparkOperator(
        task_id="raw_icca_piicix_sig",
        name="raw-icca-piicix-sig",
        arguments=["config/prod.conf", "default", "raw_icca_piicix_sig", '{{ds}}'],
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_config="medium-etl",
        dag=dag
    )

    icca_piicix_sig_calibre = SparkOperator(
        task_id="raw_icca_piicix_sig_calibre",
        name="raw-icca-piicix-sig-calibre",
        arguments=["config/prod.conf", "default", "raw_icca_piicix_sig_calibre", '{{ds}}'],
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_config="medium-etl",
        dag=dag
    )

    icca_piicix_alertes = SparkOperator(
        task_id="raw_icca_piicix_alertes",
        name="raw-icca-piicix-alertes",
        arguments=["config/prod.conf", "default", "raw_icca_piicix_alertes", '{{ds}}'],
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_config="medium-etl",
        dag=dag
    )

    end = EmptyOperator(
        task_id="publish_ingestion_neonat_cathydb",
        on_success_callback=Slack.notify_dag_completion,
        on_failure_callback=Slack.notify_task_failure
    )

    # start >> [icca_external_numeric, icca_external_wave, icca_external_patient, icca_piicix_num, icca_piicix_sig,
    #           icca_piicix_sig_calibre, icca_piicix_alertes] >> end
    start >> [icca_piicix_num, icca_piicix_sig, icca_piicix_sig_calibre, icca_piicix_alertes] >> end
