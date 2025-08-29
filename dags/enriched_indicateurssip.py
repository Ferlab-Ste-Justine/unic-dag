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

from lib.config import DEFAULT_PARAMS, DEFAULT_TIMEOUT_HOURS, DEFAULT_ARGS, SPARK_FAILURE_MSG, GREEN_MINIO_CONN_ID
from lib.operators.copy_csv_to_postgres import CopyCsvToPostgres
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.notify import start, end

JAR = 's3a://spark-prd/jars/unic-etl-{{ params.branch }}.jar'

DOC = """
# Enriched Indicateurs SIP

ETL enriched pour le projet Inidicateurs SIP. 

### Description
Cet ETL met à jour d'une façon hebdomadaire 6 tables : Sejour, Catheter, Ventilation, Extubation, Ecmo et Scores à partir de ICCA. 
Ces tables vont être utilisées pour générer les graphes Power BI pour afficher les indicateurs demandés.

### Horaire
* __Date de début__ - 19 Decembre 2023
* __Date de fin__ - aucune
* __Jour et heure__ - 22 heure de Montréal
* __Intervalle__ - Chaque Jour


"""

# Update default args
args = DEFAULT_ARGS.copy()
args.update({'trigger_rule': TriggerRule.NONE_FAILED})

dag = DAG(
    dag_id="enriched_indicateurssip",
    doc_md=DOC,
    start_date=datetime(2023, 12, 12, 18, tzinfo=pendulum.timezone("America/Montreal")),
    schedule_interval="0 22 * * *",
    params=DEFAULT_PARAMS,
    dagrun_timeout=timedelta(hours=DEFAULT_TIMEOUT_HOURS),
    default_args=args,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=1,
    max_active_tasks=4,
    tags=["enriched"],
    on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
)

with dag:

    @task_group()
    def enriched():
        enriched_zone = "yellow"
        enriched_main_class = "bio.ferlab.ui.etl.yellow.enriched.indicateurssip.Main"

        def enriched_arguments(destination: str) -> List[str]:
            return [
                destination,
                "--config", "config/prod.conf",
                "--steps", "initial",
                "--app-name", destination,
            ]


        enriched_participant_index = SparkOperator(
            task_id="enriched_indicateurssip_participant_index",
            name="enriched-indicateurssip-participant-index",
            arguments=enriched_arguments("enriched_indicateurssip_participant_index"),
            zone=enriched_zone,
            spark_class=enriched_main_class,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
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
            spark_failure_msg=SPARK_FAILURE_MSG,
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
            spark_failure_msg=SPARK_FAILURE_MSG,
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
            spark_failure_msg=SPARK_FAILURE_MSG,
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
            spark_failure_msg=SPARK_FAILURE_MSG,
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
            spark_failure_msg=SPARK_FAILURE_MSG,
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
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="small-etl",
            dag=dag,
        )

        enriched_ecmo = SparkOperator(
            task_id="enriched_indicateurssip_ecmo",
            name="enriched-indicateurssip-ecmo",
            arguments=enriched_arguments("enriched_indicateurssip_ecmo"),
            zone=enriched_zone,
            spark_class=enriched_main_class,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="small-etl",
            dag=dag,
        )

        enriched_scores = SparkOperator(
            task_id="enriched_indicateurssip_scores",
            name="enriched-indicateurssip-scores",
            arguments=enriched_arguments("enriched_indicateurssip_scores"),
            zone=enriched_zone,
            spark_class=enriched_main_class,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="small-etl",
            dag=dag,
        )

        enriched_infections = SparkOperator(
            task_id="enriched_indicateurssip_infections",
            name="enriched-indicateurssip-infections",
            arguments=enriched_arguments("enriched_indicateurssip_infections") + ["--date", "{{ logical_date }}"],
            zone=enriched_zone,
            spark_class=enriched_main_class,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="small-etl",
            dag=dag,
        )

        enriched_central_catheters_details = SparkOperator(
            task_id="enriched_indicateurssip_central_catheters_details",
            name="enriched-indicateurssip-central-catheters-details",
            arguments=enriched_arguments("enriched_indicateurssip_central_catheters_details"),
            zone=enriched_zone,
            spark_class=enriched_main_class,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="small-etl",
            dag=dag,
        )



        enriched_participant_index >> enriched_sejour >> [enriched_catheter, enriched_ventilation,
                                                          enriched_extubation, enriched_lits, enriched_infirmieres,
                                                          enriched_ecmo, enriched_scores, enriched_infections] >> enriched_central_catheters_details

    ENRICHED_GROUP = enriched()

    @task_group()
    def released():
        released_zone = "green"
        released_main_class = "bio.ferlab.ui.etl.released.Main"

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
            spark_failure_msg=SPARK_FAILURE_MSG,
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
            spark_failure_msg=SPARK_FAILURE_MSG,
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
            spark_failure_msg=SPARK_FAILURE_MSG,
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
            spark_failure_msg=SPARK_FAILURE_MSG,
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
            spark_failure_msg=SPARK_FAILURE_MSG,
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
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="small-etl",
            dag=dag,
        )

        released_ecmo = SparkOperator(
            task_id="released_indicateurssip_ecmo",
            name="released-indicateurssip-ecmo",
            arguments=released_arguments("released_indicateurssip_ecmo"),
            zone=released_zone,
            spark_class=released_main_class,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="small-etl",
            dag=dag,
        )

        released_scores = SparkOperator(
            task_id="released_indicateurssip_scores",
            name="released-indicateurssip-scores",
            arguments=released_arguments("released_indicateurssip_scores"),
            zone=released_zone,
            spark_class=released_main_class,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="small-etl",
            dag=dag,
        )

        released_infections = SparkOperator(
            task_id="released_indicateurssip_infections",
            name="released-indicateurssip-infections",
            arguments=released_arguments("released_indicateurssip_infections"),
            zone=released_zone,
            spark_class=released_main_class,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="small-etl",
            dag=dag,
        )

        released_central_catheters_details = SparkOperator(
            task_id="released_indicateurssip_central_catheters_details",
            name="released-indicateurssip-central-catheters-details",
            arguments=released_arguments("released_indicateurssip_central_catheters_details"),
            zone=released_zone,
            spark_class=released_main_class,
            spark_jar=JAR,
            spark_failure_msg=SPARK_FAILURE_MSG,
            spark_config="small-etl",
            dag=dag,
        )


        [released_sejour, released_catheter, released_ventilation, released_extubation, released_lits, released_infirmieres, released_ecmo, released_scores, released_infections, released_central_catheters_details]

    RELEASED_GROUP = released()

    @task_group()
    def published():
        ca_path = '/tmp/ca/bi/'  # must correspond to path in postgres connection string
        ca_filename = 'ca.crt'  # must correspond to filename in postgres connection string
        ca_cert = Variable.get('postgres_ca_certificate', None)

        copy_conf = [
            {"src_s3_bucket" :  "green-prd", "src_s3_key" :  "released/indicateurssip/catheter/catheter.csv"                                  , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "catheter"   }              ,
            {"src_s3_bucket" :  "green-prd", "src_s3_key" :  "released/indicateurssip/extubation/extubation.csv"                              , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "extubation" }              ,
            {"src_s3_bucket" :  "green-prd", "src_s3_key" :  "released/indicateurssip/sejour/sejour.csv"                                      , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "sejour"     }              ,
            {"src_s3_bucket" :  "green-prd", "src_s3_key" :  "released/indicateurssip/ventilation/ventilation.csv"                            , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "ventilation"}              ,
            {"src_s3_bucket" :  "green-prd", "src_s3_key" :  "released/indicateurssip/lits/lits.csv"                                          , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "lits"       }              ,
            {"src_s3_bucket" :  "green-prd", "src_s3_key" :  "released/indicateurssip/infirmieres/infirmieres.csv"                            , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "infirmieres"}              ,
            {"src_s3_bucket" :  "green-prd", "src_s3_key" :  "released/indicateurssip/ecmo/ecmo.csv"                                          , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "ecmo"}                     ,
            {"src_s3_bucket" :  "green-prd", "src_s3_key" :  "released/indicateurssip/scores/scores.csv"                                      , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "scores"}                   ,
            {"src_s3_bucket" :  "green-prd", "src_s3_key" :  "released/indicateurssip/infections/infections.csv"                              , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "infections"}               ,
            {"src_s3_bucket" :  "green-prd", "src_s3_key" :  "released/indicateurssip/central_catheters_details/central_catheters_details.csv", "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "central_catheters_details"}

        ]

        published_indicateurs_sip = CopyCsvToPostgres(
            task_id="published_indicateurs_sip",
            postgres_conn_id="postgresql_bi_rw",
            ca_path=ca_path,
            ca_filename=ca_filename,
            ca_cert=ca_cert,
            table_copy_conf=copy_conf,
            minio_conn_id=GREEN_MINIO_CONN_ID
        )

        published_indicateurs_sip

    PUBLISHED_GROUP = published()

    start() >> ENRICHED_GROUP >> RELEASED_GROUP >> PUBLISHED_GROUP >> end()
