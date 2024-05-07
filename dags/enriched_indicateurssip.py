"""
Enriched Indicateurs SIP
"""
# pylint: disable=missing-function-docstring, duplicate-code, expression-not-assigned
from datetime import datetime, timedelta
from typing import List

import pendulum
from airflow import DAG
from airflow.decorators import task_group
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

from lib.config import default_params, default_timeout_hours, default_args, spark_failure_msg
from lib.operators.copy_csv_to_postgres import CopyCsvToPostgres
from lib.operators.spark import SparkOperator
from lib.tasks.notify import start, end

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
    max_active_tasks=4,
    tags=["enriched"]
)

with dag:

    @task_group()
    def enriched():
        enriched_zone = "yellow"
        enriched_main_class = "bio.ferlab.ui.etl.yellow.enriched.indicateurssip.Main"

        def enriched_arguments(destination: str) -> List[str]:
            # !!! Do not set to initial, otherwise the participant index will be re-generated !!!
            return ["config/prod.conf", "initial", destination]


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

        enriched_lits = SparkOperator(
            task_id="enriched_indicateurssip_lits",
            name="enriched-indicateurssip-lits",
            arguments=enriched_arguments("enriched_indicateurssip_lits"),
            zone=enriched_zone,
            spark_class=enriched_main_class,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag,
        )

        enriched_infirmieres = SparkOperator(
            task_id="enriched_indicateurssip_infirmieres",
            name="enriched-indicateurssip-infirmieres",
            arguments=enriched_arguments("enriched_indicateurssip_infirmieres"),
            zone=enriched_zone,
            spark_class=enriched_main_class,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag,
        )

        enriched_participant_index >> enriched_sejour >> [enriched_catheter, enriched_ventilation,
                                                          enriched_extubation, enriched_lits, enriched_infirmieres]

    ENRICHED_GROUP = enriched()

    @task_group()
    def released():
        released_zone = "green"
        released_main_class = "bio.ferlab.ui.etl.green.released.unversioned.Main"

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
            zone=released_zone,
            spark_class=released_main_class,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag,
        )

        released_catheter = SparkOperator(
            task_id="released_indicateurssip_catheter",
            name="released-indicateurssip-catheter",
            arguments=released_arguments("released_indicateurssip_catheter"),
            zone=released_zone,
            spark_class=released_main_class,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag,
        )

        released_ventilation = SparkOperator(
            task_id="released_indicateurssip_ventilation",
            name="released-indicateurssip-ventilation",
            arguments=released_arguments("released_indicateurssip_ventilation"),
            zone=released_zone,
            spark_class=released_main_class,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag,
        )

        released_extubation = SparkOperator(
            task_id="released_indicateurssip_extubation",
            name="released-indicateurssip-extubation",
            arguments=released_arguments("released_indicateurssip_extubation"),
            zone=released_zone,
            spark_class=released_main_class,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag,
        )

        released_lits = SparkOperator(
            task_id="released_indicateurssip_lits",
            name="released-indicateurssip-lits",
            arguments=released_arguments("released_indicateurssip_lits"),
            zone=released_zone,
            spark_class=released_main_class,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag,
        )

        released_infirmieres = SparkOperator(
            task_id="released_indicateurssip_infirmieres",
            name="released-indicateurssip-infirmieres",
            arguments=released_arguments("released_indicateurssip_infirmieres"),
            zone=released_zone,
            spark_class=released_main_class,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="small-etl",
            dag=dag,
        )

        [released_sejour, released_catheter, released_ventilation, released_extubation, released_lits, released_infirmieres]

    RELEASED_GROUP = released()

    @task_group()
    def published():
        ca_path = '/tmp/ca/bi/'  # must correspond to path in postgres connection string
        ca_filename = 'ca.crt'  # must correspond to filename in postgres connection string
        ca_cert = Variable.get('postgres_ca_certificate', None)

        copy_conf = [
            {"src_s3_bucket" :  "green-prd", "src_s3_key" :  "released/indicateurssip/catheter/catheter.csv"      , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "catheter"   },
            {"src_s3_bucket" :  "green-prd", "src_s3_key" :  "released/indicateurssip/extubation/extubation.csv"  , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "extubation" },
            {"src_s3_bucket" :  "green-prd", "src_s3_key" :  "released/indicateurssip/sejour/sejour.csv"          , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "sejour"     },
            {"src_s3_bucket" :  "green-prd", "src_s3_key" :  "released/indicateurssip/ventilation/ventilation.csv", "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "ventilation"},
            {"src_s3_bucket" :  "green-prd", "src_s3_key" :  "released/indicateurssip/lits/lits.csv"              , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "lits"       },
            {"src_s3_bucket" :  "green-prd", "src_s3_key" :  "released/indicateurssip/infirmieres/infirmieres.csv", "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "infirmieres"}
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

        published_indicateurs_sip

    PUBLISHED_GROUP = published()

    start() >> ENRICHED_GROUP >> RELEASED_GROUP >> PUBLISHED_GROUP >> end()
