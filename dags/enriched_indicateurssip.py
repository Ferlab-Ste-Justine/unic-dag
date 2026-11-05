"""
Enriched Indicateurs SIP
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task_group
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

from lib.config import DEFAULT_PARAMS, DEFAULT_TIMEOUT_HOURS, DEFAULT_ARGS, SPARK_FAILURE_MSG, \
    GREEN_MINIO_CONN_ID, JAR, CONFIG_FILE, RELEASED_BUCKET, LOCAL_TZ, LOGICAL_DATE
from lib.operators.copy_csv_to_postgres import CopyCsvToPostgres
from lib.slack import Slack
from tasks import create_tasks

DOC = """
# Enriched Indicateurs SIP

ETL enriched pour le projet Inidicateurs SIP. 

### Description
Cet ETL met à jour d'une façon hebdomadaire 6 tables : Sejour, Catheter, Ventilation, Extubation, Ecmo et Scores à partir de ICCA. 
Ces tables vont être utilisées pour générer les graphes Power BI pour afficher les indicateurs demandés.

### Tests QA
* `enriched_indicateurssip_unmapped_stringmap_values` - Détecte les valeurs sources non nulles qui ne sont pas mappées par les string maps.
En cas d'échec, la tâche est ignorée (skipped) et une alerte Slack est envoyée, pour ne pas bloquer la publication.

### Horaire
* __Date de début__ - 19 Decembre 2023
* __Date de fin__ - aucune
* __Jour et heure__ - 22h heure de Montréal
* __Intervalle__ - Chaque Jour
"""

CONFIG = {
    "steps": [
        {
            "destination_zone": "yellow",
            "destination_subzone": "enriched",
            "main_class": "bio.ferlab.ui.etl.yellow.enriched.indicateurssip.Main",
            "multiple_main_methods": True,
            "datasets": [
                {"dataset_id": "enriched_indicateurssip_participant_index"        , "cluster_type": "small", "run_type": "initial", "pass_date": False, "dependencies": []},
                {"dataset_id": "enriched_indicateurssip_sejour"                   , "cluster_type": "small", "run_type": "initial", "pass_date": False, "dependencies": ["enriched_indicateurssip_participant_index"]},
                {"dataset_id": "enriched_indicateurssip_catheter"                 , "cluster_type": "small", "run_type": "initial", "pass_date": False, "dependencies": ["enriched_indicateurssip_sejour"]},
                {"dataset_id": "enriched_indicateurssip_ventilation"              , "cluster_type": "small", "run_type": "initial", "pass_date": False, "dependencies": ["enriched_indicateurssip_catheter"]},
                {"dataset_id": "enriched_indicateurssip_extubation"               , "cluster_type": "small", "run_type": "initial", "pass_date": False, "dependencies": ["enriched_indicateurssip_catheter"]},
                {"dataset_id": "enriched_indicateurssip_lits"                     , "cluster_type": "small", "run_type": "initial", "pass_date": False, "dependencies": ["enriched_indicateurssip_catheter"]},
                {"dataset_id": "enriched_indicateurssip_infirmieres"              , "cluster_type": "small", "run_type": "initial", "pass_date": False, "dependencies": ["enriched_indicateurssip_catheter"]},
                {"dataset_id": "enriched_indicateurssip_ecmo"                     , "cluster_type": "small", "run_type": "initial", "pass_date": False, "dependencies": ["enriched_indicateurssip_catheter"]},
                {"dataset_id": "enriched_indicateurssip_scores"                   , "cluster_type": "small", "run_type": "initial", "pass_date": False, "dependencies": ["enriched_indicateurssip_catheter"]},
                {"dataset_id": "enriched_indicateurssip_infections"               , "cluster_type": "small", "run_type": "initial", "pass_date": False, "dependencies": ["enriched_indicateurssip_catheter"]},
                {"dataset_id": "enriched_indicateurssip_demandes_lits"            , "cluster_type": "small", "run_type": "initial", "pass_date": False, "dependencies": ["enriched_indicateurssip_catheter"]},
                {"dataset_id": "enriched_indicateurssip_toutes_les_infections"    , "cluster_type": "small", "run_type": "initial", "pass_date": False, "dependencies": ["enriched_indicateurssip_catheter"]},
                {"dataset_id": "enriched_indicateurssip_central_catheters_details", "cluster_type": "small", "run_type": "initial", "pass_date": False, "dependencies": ["enriched_indicateurssip_ventilation", "enriched_indicateurssip_extubation", "enriched_indicateurssip_lits", "enriched_indicateurssip_infirmieres", "enriched_indicateurssip_ecmo", "enriched_indicateurssip_scores", "enriched_indicateurssip_infections", "enriched_indicateurssip_demandes_lits", "enriched_indicateurssip_toutes_les_infections"]},
            ],
            "post_tests": [
                {"name": "unmapped_stringmap_values", "destinations": ["enriched_indicateurssip_sejour"], "cluster_type": "xsmall", "blocking": False},
            ]
        },
        {
            "destination_zone": "green",
            "destination_subzone": "released",
            "datasets": [
                {"dataset_id": "released_indicateurssip_sejour"                   , "cluster_type": "small", "run_type": "default", "pass_date": False, "dependencies": []},
                {"dataset_id": "released_indicateurssip_catheter"                 , "cluster_type": "small", "run_type": "default", "pass_date": False, "dependencies": []},
                {"dataset_id": "released_indicateurssip_ventilation"              , "cluster_type": "small", "run_type": "default", "pass_date": False, "dependencies": []},
                {"dataset_id": "released_indicateurssip_extubation"               , "cluster_type": "small", "run_type": "default", "pass_date": False, "dependencies": []},
                {"dataset_id": "released_indicateurssip_lits"                     , "cluster_type": "small", "run_type": "default", "pass_date": False, "dependencies": []},
                {"dataset_id": "released_indicateurssip_infirmieres"              , "cluster_type": "small", "run_type": "default", "pass_date": False, "dependencies": []},
                {"dataset_id": "released_indicateurssip_ecmo"                     , "cluster_type": "small", "run_type": "default", "pass_date": False, "dependencies": []},
                {"dataset_id": "released_indicateurssip_scores"                   , "cluster_type": "small", "run_type": "default", "pass_date": False, "dependencies": []},
                {"dataset_id": "released_indicateurssip_infections"               , "cluster_type": "small", "run_type": "default", "pass_date": False, "dependencies": []},
                {"dataset_id": "released_indicateurssip_central_catheters_details", "cluster_type": "small", "run_type": "default", "pass_date": False, "dependencies": []},
                {"dataset_id": "released_indicateurssip_demandes_lits"            , "cluster_type": "small", "run_type": "default", "pass_date": False, "dependencies": []},
                {"dataset_id": "released_indicateurssip_toutes_les_infections"    , "cluster_type": "small", "run_type": "default", "pass_date": False, "dependencies": []},
            ]
        }
    ]
}

# Update default args
args = DEFAULT_ARGS.copy()
args.update({'trigger_rule': TriggerRule.NONE_FAILED})

dag = DAG(
    dag_id="enriched_indicateurssip",
    doc_md=DOC,
    start_date=datetime(2023, 12, 12, 18, tzinfo=LOCAL_TZ),
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
    create_tasks(dag=dag, dag_config=CONFIG, config_file=CONFIG_FILE, jar=JAR, resource="indicateurssip",
                 spark_failure_msg=SPARK_FAILURE_MSG)

    # InfectionsETL expects the logical date as an ISO date-time, which pass_date does not provide
    dag.get_task("enriched.enriched_indicateurssip_infections").arguments += ["--date", LOGICAL_DATE]

    @task_group()
    def published():
        ca_path = '/tmp/ca/bi/'  # must correspond to path in postgres connection string
        ca_filename = 'ca.crt'  # must correspond to filename in postgres connection string
        ca_cert = Variable.get('postgres_ca_certificate', None)

        copy_conf = [
            {"src_s3_bucket" :  RELEASED_BUCKET, "src_s3_key" :  "released/indicateurssip/catheter/catheter.csv"                                  , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "catheter"},
            {"src_s3_bucket" :  RELEASED_BUCKET, "src_s3_key" :  "released/indicateurssip/extubation/extubation.csv"                              , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "extubation"},
            {"src_s3_bucket" :  RELEASED_BUCKET, "src_s3_key" :  "released/indicateurssip/sejour/sejour.csv"                                      , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "sejour"},
            {"src_s3_bucket" :  RELEASED_BUCKET, "src_s3_key" :  "released/indicateurssip/ventilation/ventilation.csv"                            , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "ventilation"},
            {"src_s3_bucket" :  RELEASED_BUCKET, "src_s3_key" :  "released/indicateurssip/lits/lits.csv"                                          , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "lits"},
            {"src_s3_bucket" :  RELEASED_BUCKET, "src_s3_key" :  "released/indicateurssip/infirmieres/infirmieres.csv"                            , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "infirmieres"},
            {"src_s3_bucket" :  RELEASED_BUCKET, "src_s3_key" :  "released/indicateurssip/ecmo/ecmo.csv"                                          , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "ecmo"},
            {"src_s3_bucket" :  RELEASED_BUCKET, "src_s3_key" :  "released/indicateurssip/scores/scores.csv"                                      , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "scores"},
            {"src_s3_bucket" :  RELEASED_BUCKET, "src_s3_key" :  "released/indicateurssip/infections/infections.csv"                              , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "infections"},
            {"src_s3_bucket" :  RELEASED_BUCKET, "src_s3_key" :  "released/indicateurssip/central_catheters_details/central_catheters_details.csv", "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "central_catheters_details"},
            {"src_s3_bucket" :  RELEASED_BUCKET, "src_s3_key" :  "released/indicateurssip/demandes_lits/demandes_lits.csv"                        , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "demandes_lits"},
            {"src_s3_bucket" :  RELEASED_BUCKET, "src_s3_key" :  "released/indicateurssip/toutes_les_infections/toutes_les_infections.csv"        , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "toutes_les_infections"},
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

    dag.get_task("released.end_released_indicateurssip") >> published()
