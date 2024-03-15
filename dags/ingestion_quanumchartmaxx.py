"""
DAG to ingest data from quanum
"""
# pylint: disable=missing-function-docstring, duplicate-code


from datetime import timedelta, datetime
from typing import List
import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from core.slack import Slack
from core.config import default_params, default_args, spark_failure_msg, jar

from operators.spark import SparkOperator

from spark_operators import sanitize_string

DOC = """
    DAG to ingest data from Quanum -> perform data union with Chartmaxx system data -> anonymize the result.
    Dag scheduled to run daily at 19:00.
"""

QUANUM_CURATED_ZONE = "red"
QUANUMCHARTMAXX_CURATED_ZONE = "red"
QUANUMCHARTMAXX_ANONYMIZED_ZONE = "yellow"
QUANUM_CURATED_MAIN_CLASS = "bio.ferlab.ui.etl.red.curated.quanum.Main"
QUANUMCHARTMAXX_ANONYMIZED_MAIN_CLASS = "bio.ferlab.ui.etl.yellow.anonymized.Main"
QUANUMCHARTMAXX_CURATED_MAIN_CLASS = "bio.ferlab.ui.etl.red.curated.quanumchartmaxx.Main"

args = default_args.copy()
LOCAL_TZ = pendulum.timezone("America/Montreal")

dag = DAG(
    dag_id="ingestion_quanumchartmaxx",
    doc_md=DOC,
    schedule_interval="0 19 * * *",
    start_date=datetime(2024, 3, 15, tzinfo=LOCAL_TZ),
    params=default_params,
    dagrun_timeout=timedelta(hours=20),
    default_args=args,
    is_paused_upon_creation=True,
    catchup=False,
    concurrency=4,
    tags=["curated"]
)

def generate_spark_arguments(destination: str, steps: str = "default", etl_version: str= "v2") -> List[str]:
    """
    Generate Spark task arguments for the ETL process.
    """
    if etl_version == "v2":
        return ["config/prod.cond", steps, destination]
    return [
        "--config", "config/prod.conf",
        "--steps", steps,
        "--app-name", destination,
        "--destination", destination,
        "--date", "{{ds}}" # not needed for the moment but i can use it later
    ]

with dag:

    start_curated_quanum_task = EmptyOperator(
        task_id="start_curated_quanum_task",
        on_execute_callback=Slack.notify_dag_start
    )

    start_curated_quanumchartmaxx_task = EmptyOperator(
        task_id="start_curated_quanumchartmaxx_task",
        on_execute_callback=Slack.notify_dag_start
    )

    start_anonymized_quanumchartmaxx_task = EmptyOperator(
        task_id="start_anonymized_quanumchartmaxx_task",
        on_execute_callback=Slack.notify_dag_start
    )

    quanum_curated_tasks = [
        ("curated_quanum_form_metadata_vw"                                   , "small-etl")  ,
        ("curated_quanum_form_name_vw"                                       , "small-etl")  ,
        ("curated_quanum_a*"                                                 , "medium-etl") ,
        ("curated_quanum_b*"                                                 , "small-etl")  ,
        ("curated_quanum_c*"                                                 , "medium-etl") ,
        ("curated_quanum_dossier*"                                           , "small-etl")  ,
        ("curated_quanum_e*"                                                 , "small-etl")  ,
        ("curated_quanum_f*"                                                 , "small-etl")  ,
        ("curated_quanum_g*"                                                 , "small-etl")  ,
        ("curated_quanum_i*"                                                 , "small-etl")  ,
        ("curated_quanum_l*"                                                 , "small-etl")  ,
        ("curated_quanum_maladie*"                                           , "medium-etl") ,
        ("curated_quanum_n*"                                                 , "small-etl")  ,
        ("curated_quanum_o*"                                                 , "small-etl")  ,
        ("curated_quanum_parental_quest_on_asthma"                           , "small-etl")  ,
        ("curated_quanum_pediatrie_evaluation_inf_vaccination"               , "small-etl")  ,
        ("curated_quanum_pharmacie_fibrose_kystique_conseils_au_patient"     , "small-etl")  ,
        ("curated_quanum_pharmacie_fibrose_kystique_histoire_medicamenteuse" , "small-etl")  ,
        ("curated_quanum_physio_eval_marche_quise"                           , "small-etl")  ,
        ("curated_quanum_physio_eval_neuro_motrice"                          , "small-etl")  ,
        ("curated_quanum_physio_evaluation_initiale"                         , "small-etl")  ,
        ("curated_quanum_physio_evaluation_torticolis_plagio"                , "small-etl")  ,
        ("curated_quanum_physio_notes_de_conge"                              , "small-etl")  ,
        ("curated_quanum_physio_notes_devolution"                            , "small-etl")  ,
        ("curated_quanum_physio_notes_dintervention"                         , "small-etl")  ,
        ("curated_quanum_physio_rapport_fermeture"                           , "small-etl")  ,
        ("curated_quanum_physio_reeval_neuro_motrice"                        , "small-etl")  ,
        ("curated_quanum_physio_triage_torticoli_plagio"                     , "small-etl")  ,
        ("curated_quanum_plan_daction_asthme_anglais"                        , "small-etl")  ,
        ("curated_quanum_plan_daction_crises_asthme_urgence"                 , "small-etl")  ,
        ("curated_quanum_plan_daction_pour_lasthme"                          , "small-etl")  ,
        ("curated_quanum_plan_daction_pour_lasthme_anglais"                  , "small-etl")  ,
        ("curated_quanum_plan_enseig_clientele_obst_3e_trim"                 , "small-etl")  ,
        ("curated_quanum_plastie_externe_consultation_medicale"              , "small-etl")  ,
        ("curated_quanum_plastie_externe_evaluation_infirmiere"              , "small-etl")  ,
        ("curated_quanum_plateau_ambulatoire_note_inf"                       , "small-etl")  ,
        ("curated_quanum_pneumo_enseignement_infirmier"                      , "small-etl")  ,
        ("curated_quanum_pneumo_suivi_synd_detresse_resp_aigue"              , "small-etl")  ,
        ("curated_quanum_pneumo_tests_cutanes_dallergie"                     , "small-etl")  ,
        ("curated_quanum_pneumologie_consultation_initiale"                  , "small-etl")  ,
        ("curated_quanum_precaution_renale_suivi_medical"                    , "small-etl")  ,
        ("curated_quanum_prescription_de_protheses_et_d_ortheses"            , "small-etl")  ,
        ("curated_quanum_prog_soins_complexes_psic_infirmiere"               , "small-etl")  ,
        ("curated_quanum_q*"                                                 , "small-etl")  ,
        ("curated_quanum_r*"                                                 , "medium-etl") ,
        ("curated_quanum_s*"                                                 , "small-etl")  ,
        ("curated_quanum_t*"                                                 , "medium-etl") ,
        ("curated_quanum_urogynecologie*"                                    , "medium-etl") ,
        ("curated_quanum_v*"                                                 , "small-etl")
    ]

    quanumchartmaxx_curated_tasks = [
        ("curated_quanum_chartmaxx_a*"              , "medium-etl") ,
        ("curated_quanum_chartmaxx_b*"              , "small-etl")  ,
        ("curated_quanum_chartmaxx_c*"              , "medium-etl") ,
        ("curated_quanum_chartmaxx_dossier*"        , "small-etl")  ,
        ("curated_quanum_chartmaxx_e*"              , "small-etl")  ,
        ("curated_quanum_chartmaxx_f*"              , "small-etl")  ,
        ("curated_quanum_chartmaxx_g*"              , "small-etl")  ,
        ("curated_quanum_chartmaxx_i*"              , "small-etl")  ,
        ("curated_quanum_chartmaxx_l*"              , "small-etl")  ,
        ("curated_quanum_chartmaxx_maladie*"        , "small-etl")  ,
        ("curated_quanum_chartmaxx_n*"              , "medium-etl") ,
        ("curated_quanum_chartmaxx_o*"              , "medium-etl") ,
        ("curated_quanum_chartmaxx_p*"              , "medium-etl") ,
        ("curated_quanum_chartmaxx_q*"              , "small-etl")  ,
        ("curated_quanum_chartmaxx_r*"              , "medium-etl") ,
        ("curated_quanum_chartmaxx_s*"              , "small-etl")  ,
        ("curated_quanum_chartmaxx_t*"              , "medium-etl") ,
        ("curated_quanum_chartmaxx_urogynecologie*" , "medium-etl") ,
        ("curated_quanum_chartmaxx_v*"              , "small-etl")
    ]

    quanumchartmaxx_anonymized_tasks = [
        ("anonymized_quanum_chartmaxx_a*"                                   , "small-etl") ,
        ("anonymized_quanum_chartmaxx_childhood_asthma_test_4_11_years_old" , "small-etl") ,
        ("anonymized_quanum_chartmaxx_dossier*"                             , "small-etl") ,
        ("anonymized_quanum_chartmaxx_p*"                                   , "small-etl") ,
        ("anonymized_quanum_chartmaxx_clinique_*"                           , "small-etl")
    ]

    start_curated_quanum = [SparkOperator(
        task_id=sanitize_string(task_name, "_"),
        name=sanitize_string(task_name[:40], '-'),
        arguments=generate_spark_arguments(task_name),
        zone=QUANUM_CURATED_ZONE,
        spark_class=QUANUM_CURATED_MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config=cluster_size,
        dag=dag
    ) for task_name, cluster_size in quanum_curated_tasks]

    start_curated_quanumchartmaxx = [SparkOperator(
        task_id=sanitize_string(task_name, "_"),
        name=sanitize_string(task_name[:40], '-'),
        arguments=generate_spark_arguments(task_name, "initial", "v4"),
        zone=QUANUMCHARTMAXX_CURATED_ZONE,
        spark_class=QUANUMCHARTMAXX_CURATED_MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config=cluster_size,
        dag=dag
    ) for task_name, cluster_size in quanumchartmaxx_curated_tasks]

    start_anonymized_quanumchartmaxx = [SparkOperator(
        task_id=sanitize_string(task_name, "_"),
        name=sanitize_string(task_name[:40], '-'),
        arguments=generate_spark_arguments(task_name, "initial"),
        zone=QUANUMCHARTMAXX_ANONYMIZED_ZONE,
        spark_class=QUANUMCHARTMAXX_ANONYMIZED_MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config=cluster_size,
        dag=dag
    ) for task_name, cluster_size in quanumchartmaxx_anonymized_tasks]

    publish_anonymized_quanumchartmaxx = EmptyOperator(
        task_id="publish_anonymized_quanum",
        on_success_callback=Slack.notify_dag_completion
    )

    start_curated_quanum_task >> start_curated_quanum >> start_curated_quanumchartmaxx_task >> start_curated_quanumchartmaxx >> start_anonymized_quanumchartmaxx_task >> start_anonymized_quanumchartmaxx >> publish_anonymized_quanumchartmaxx
