"""
DAG pour le parsing des messages HL7 de Radimage
"""
# pylint: disable=duplicate-code, expression-not-assigned
from datetime import datetime, timedelta

import pendulum
from airflow import DAG

from lib.config import default_params, default_args, spark_failure_msg, jar
from lib.operators.spark import SparkOperator
from lib.tasks.notify import start, end

DOC = """
# Curated Radimage HL7 DAG

ETL curated pour parser les messages HL7 radimage déposé en zone rouge

### Description
Cet ETL roule pour parser les messages HL7 et les convertir de messages .hl7 au format Delta. 
Cet ETL roule 1 fois par jour.
Elle parse des données de la date précédante de la date de la run dans airflow, par exemple:
La run du 2 janvier 2020 parse les données du 1 janvier dans le lac.

"""

ANONYMIZED_ZONE = "yellow"
CURATED_ZONE = "red"
ANONYMIZED_MAIN_CLASS = "bio.ferlab.ui.etl.yellow.anonymized.Main"
CURATED_MAIN_CLASS = "bio.ferlab.ui.etl.red.curated.hl7.Main"
args = default_args.copy()
args.update({
    'provide_context': True})

dag = DAG(
    dag_id="curated_radimage_hl7",
    doc_md=DOC,
    start_date=datetime(2013, 10, 5, 7, tzinfo=pendulum.timezone("America/Montreal")),
    schedule_interval=timedelta(days=1),
    params=default_params,
    dagrun_timeout=timedelta(hours=2),
    default_args=args,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=1,
    max_active_tasks=3,
    tags=["curated"]
)

with dag:

    radimage_hl7_curated_tasks = [
        ("curated_radimage_hl7_oru_r01_al1", "small-etl"),
        ("curated_radimage_hl7_oru_r01_nte", "small-etl"),
        ("curated_radimage_hl7_oru_r01_obr", "small-etl"),
        ("curated_radimage_hl7_oru_r01_obx", "small-etl"),
        ("curated_radimage_hl7_oru_r01_orc", "small-etl"),
        ("curated_radimage_hl7_oru_r01_pid", "small-etl"),
        ("curated_radimage_hl7_oru_r01_pv1", "small-etl"),
        ("curated_radimage_hl7_oru_r01_zal", "small-etl"),
        ("curated_radimage_hl7_oru_r01_zbr", "small-etl"),
        ("curated_radimage_hl7_oru_r01_zdc", "small-etl"),
        ("curated_radimage_hl7_oru_r01_zrc", "small-etl"),
    ]

    radimage_hl7_curated = [SparkOperator(
        task_id=task_name,
        name=task_name.replace("_","-"),
        arguments=["config/prod.conf", "default", task_name, '{{ds}}'],
        zone=CURATED_ZONE,
        spark_class=CURATED_MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config=cluster_size,
        dag=dag
    ) for task_name, cluster_size in radimage_hl7_curated_tasks]

    # radimage_hl7_anonymized_tasks = [
    #     ("anonymized_radimage_hl7_oru_r01_al1", "small-etl"),
    #     ("anonymized_radimage_hl7_oru_r01_nte", "small-etl"),
    #     ("anonymized_radimage_hl7_oru_r01_obr", "small-etl"),
    #     ("anonymized_radimage_hl7_oru_r01_obx", "small-etl"),
    #     ("anonymized_radimage_hl7_oru_r01_orc", "small-etl"),
    #     ("anonymized_radimage_hl7_oru_r01_pid", "small-etl"),
    #     ("anonymized_radimage_hl7_oru_r01_pv1", "small-etl"),
    #     ("anonymized_radimage_hl7_oru_r01_zal", "small-etl"),
    #     ("anonymized_radimage_hl7_oru_r01_zbr", "small-etl"),
    #     ("anonymized_radimage_hl7_oru_r01_zdc", "small-etl"),
    #     ("anonymized_radimage_hl7_oru_r01_zrc", "small-etl"),
    # ]
    #
    # radimage_hl7_anonymized = [SparkOperator(
    #     task_id=task_name,
    #     name=task_name.replace("_","-"),
    #     arguments=["config/prod.conf", "default", task_name, '{{ds}}'],
    #     zone=ANONYMIZED_ZONE,
    #     spark_class=ANONYMIZED_MAIN_CLASS,
    #     spark_jar=jar,
    #     spark_failure_msg=spark_failure_msg,
    #     spark_config=cluster_size,
    #     dag=dag
    # ) for task_name, cluster_size in radimage_hl7_anonymized_tasks]

    start("start_curated_radimage_hl7") >> radimage_hl7_curated >> end("end_curated_radimage_hl7")
