"""
Enriched Indicateurs SIP
"""
# pylint: disable=missing-function-docstring, duplicate-code
from datetime import datetime, timedelta
from typing import List

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import task_group

from core.config import default_params, default_timeout_hours, default_args, spark_failure_msg
from core.slack import Slack
from operators.copy_csv_to_postgres import CopyCsvToPostgres
from operators.spark import SparkOperator

JAR = 's3a://spark-prd/jars/unic-etl-{{ params.branch }}.jar'

DOC = """
# Enriched Indicateurs SIP

ETL enriched pour le projet Inidicateurs SIP. 

### Description
Cet ETL met à jour d'une façon hebdomadaire 4 tables : Sejour, Catheter, Ventilation et Extubation à partir de ICCA. 
Ces tables vont être utilisées pour générer les graphes Power BI pour afficher les indicateurs demandés.

### Horaire
* __Date de début__ - 19 Decembre 2023
* __Date de fin__ - aucune
* __Jour et heure__ - Mardi, 18 heure de Montréal
* __Intervalle__ - Chaque semaine


"""

# Update default args
args = default_args.copy()
args.update({'trigger_rule': TriggerRule.NONE_FAILED})

dag = DAG(
    dag_id="enriched_indicateurssip",
    doc_md=DOC,
    start_date=datetime(2023, 12, 12, 18, tzinfo=pendulum.timezone("America/Montreal")),
    schedule_interval=timedelta(weeks=1),
    params=default_params,
    dagrun_timeout=timedelta(hours=default_timeout_hours),
    default_args=args,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["enriched"]
)

with dag:

    start = EmptyOperator(
        task_id="start",
        on_execute_callback=Slack.notify_dag_start
    )

    @task_group()
    def enriched():
        enriched_zone = "yellow"
        enriched_main_class = "bio.ferlab.ui.etl.yellow.enriched.indicateurssip.Main"

        def enriched_arguments(destination: str) -> List[str]:
            # !!! Do not set to initial, otherwise the participant index will be re-generated !!!
            return ["config/prod.conf", "default", destination]


        enriched_participant_index = SparkOperator(
            task_id="enriched_indicateurssip_participant_index",
            name="enriched-indicateurssip-participant-index",
            arguments=enriched_arguments("enriched_indicateurssip_participant_index"),
            zone=enriched_zone,
            spark_class=enriched_main_class,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag
        )

        enriched_sejour = SparkOperator(
            task_id="enriched_indicateurssip_sejour",
            name="enriched-indicateurssip-sejour",
            arguments=enriched_arguments("enriched_indicateurssip_sejour"),
            zone=enriched_zone,
            spark_class=enriched_main_class,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag,
        )

        enriched_catheter = SparkOperator(
            task_id="enriched_indicateurssip_catheter",
            name="enriched-indicateurssip-catheter",
            arguments=enriched_arguments("enriched_indicateurssip_catheter"),
            zone=enriched_zone,
            spark_class=enriched_main_class,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag,
        )

        enriched_ventilation = SparkOperator(
            task_id="enriched_indicateurssip_ventilation",
            name="enriched-indicateurssip-ventilation",
            arguments=enriched_arguments("enriched_indicateurssip_ventilation"),
            zone=enriched_zone,
            spark_class=enriched_main_class,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag,
        )

        enriched_extubation = SparkOperator(
            task_id="enriched_indicateurssip_extubation",
            name="enriched-indicateurssip-extubation",
            arguments=enriched_arguments("enriched_indicateurssip_extubation"),
            zone=enriched_zone,
            spark_class=enriched_main_class,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag,
        )

        enriched_participant_index >> enriched_sejour >> [enriched_catheter, enriched_ventilation, enriched_extubation]

    @task_group()
    def released():
        RELEASED_ZONE = "green"
        RELEASED_MAIN_CLASS = "bio.ferlab.ui.etl.green.released.unversioned.Main"

        def released_arguments(destination: str, steps: str = "default") -> List[str]:
            """
            Generate Spark task arguments for the released ETL process
            """
            return [
                "--config", "config/prod.conf",
                "--steps", steps,
                "--app-name", destination,
                "--destination", destination
            ]


        released_sejour = SparkOperator(
            task_id="released_indicateurssip_sejour",
            name="released-indicateurssip-sejour",
            arguments=released_arguments("released_indicateurssip_sejour"),
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag,
        )

        released_catheter = SparkOperator(
            task_id="released_indicateurssip_catheter",
            name="released-indicateurssip-catheter",
            arguments=released_arguments("released_indicateurssip_catheter"),
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag,
        )

        released_ventilation = SparkOperator(
            task_id="released_indicateurssip_ventilation",
            name="released-indicateurssip-ventilation",
            arguments=released_arguments("released_indicateurssip_ventilation"),
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag,
        )

        released_extubation = SparkOperator(
            task_id="released_indicateurssip_extubation",
            name="released-indicateurssip-extubation",
            arguments=released_arguments("released_indicateurssip_extubation"),
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag,
        )

    @task_group()
    def published():
        ca_path = '/tmp/ca/bi/'  # must correspond to path in postgres connection string
        ca_filename = 'ca.crt'  # must correspond to filename in postgres connection string
        ca_cert = Variable.get('postgres_ca_certificate', None)

        copy_conf = [
            {"src_s3_bucket" :  "green-prd", "src_s3_key" :  "released/indicateurssip/catheter/catheter.csv"      , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "catheter"   },
            {"src_s3_bucket" :  "green-prd", "src_s3_key" :  "released/indicateurssip/extubation/extubation.csv"  , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "extubation" },
            {"src_s3_bucket" :  "green-prd", "src_s3_key" :  "released/indicateurssip/sejour/sejour.csv"          , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "sejour"     },
            {"src_s3_bucket" :  "green-prd", "src_s3_key" :  "released/indicateurssip/ventilation/ventilation.csv", "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "ventilation"}
        ]

        published_indicateurs_sip = CopyCsvToPostgres(
            task_id="published_indicateurs_sip",
            postgres_conn_id="postgresql_bi_rw",
            ca_path=ca_path,
            ca_filename=ca_filename,
            ca_cert=ca_cert,
            table_copy_conf=copy_conf,
            minio_conn_id="green_minio"
        )

    end = EmptyOperator(
        task_id="end",
        on_success_callback=Slack.notify_dag_completion
    )

    start >> enriched() >> released() >> published() >> end
