"""
Participant index anonymization DAG
"""
# pylint: disable=missing-function-docstring, duplicate-code
from datetime import timedelta, datetime

from airflow import DAG
from airflow.models import Param

from lib.config import config_file, jar, version, spark_failure_msg, default_args, default_params
from spark_operators import setup_dag

config = {
    "concurrency": 2,
    "schedule": None,
    "timeout_hours": 1,
    "steps": [{
        "destination_zone": "yellow",
        "destination_subzone": "anonymized",
        "main_class": "bio.ferlab.ui.etl.yellow.anonymized.Main",
        "multiple_main_methods": False,
        "publish_class": "",
        "schemas": [],
        "datasets":
            [
                {"dataset_id": "anonymized_unic_participant_index_atoepilot"                                         , "cluster_type": "xsmall", "run_type": "initial", "cluster_specs": {}, "dependencies": [], "pre_tests": [], "post_tests": []},
                {"dataset_id": "anonymized_unic_participant_index_barsop00_cesarienne_avec_infection"                , "cluster_type": "xsmall", "run_type": "initial", "cluster_specs": {}, "dependencies": [], "pre_tests": [], "post_tests": []},
                {"dataset_id": "anonymized_unic_participant_index_barsop00_cesarienne_planifiee"                     , "cluster_type": "xsmall", "run_type": "initial", "cluster_specs": {}, "dependencies": [], "pre_tests": [], "post_tests": []},
                {"dataset_id": "anonymized_unic_participant_index_barsop00_cesarienne_urgente"                       , "cluster_type": "xsmall", "run_type": "initial", "cluster_specs": {}, "dependencies": [], "pre_tests": [], "post_tests": []},
                {"dataset_id": "anonymized_unic_participant_index_cardiopathie"                                      , "cluster_type": "xsmall", "run_type": "default", "cluster_specs": {}, "dependencies": [], "pre_tests": [], "post_tests": []},
                {"dataset_id": "anonymized_unic_participant_index_coprema"                                           , "cluster_type": "xsmall", "run_type": "default", "cluster_specs": {}, "dependencies": [], "pre_tests": [], "post_tests": []},
                {"dataset_id": "anonymized_unic_participant_index_ivado_prf3_biopsie_foie_2023"                      , "cluster_type": "xsmall", "run_type": "default", "cluster_specs": {}, "dependencies": [], "pre_tests": [], "post_tests": []},
                {"dataset_id": "anonymized_unic_participant_index_ivado_prf3_biopsie_foie2_2023"                     , "cluster_type": "xsmall", "run_type": "default", "cluster_specs": {}, "dependencies": [], "pre_tests": [], "post_tests": []},
                {"dataset_id": "anonymized_unic_participant_index_ivado_prf3_steatose_hepatique"                     , "cluster_type": "xsmall", "run_type": "default", "cluster_specs": {}, "dependencies": [], "pre_tests": [], "post_tests": []},
                {"dataset_id": "anonymized_unic_participant_index_monchemin"                                         , "cluster_type": "xsmall", "run_type": "default", "cluster_specs": {}, "dependencies": [], "pre_tests": [], "post_tests": []},
                {"dataset_id": "anonymized_unic_participant_index_pedicss"                                           , "cluster_type": "xsmall", "run_type": "default", "cluster_specs": {}, "dependencies": [], "pre_tests": [], "post_tests": []},
                {"dataset_id": "anonymized_unic_participant_index_picaso"                                            , "cluster_type": "xsmall", "run_type": "default", "cluster_specs": {}, "dependencies": [], "pre_tests": [], "post_tests": []},
                {"dataset_id": "anonymized_unic_participant_index_picaso_inclusion_diagnosis_treatmentplan_2012_2022", "cluster_type": "xsmall", "run_type": "default", "cluster_specs": {}, "dependencies": [], "pre_tests": [], "post_tests": []},
                {"dataset_id": "anonymized_unic_participant_index_picaso_merge_diagnosis_treatmentplan_2012_2022"    , "cluster_type": "xsmall", "run_type": "default", "cluster_specs": {}, "dependencies": [], "pre_tests": [], "post_tests": []},
                {"dataset_id": "anonymized_unic_participant_index_predisepsis"                                       , "cluster_type": "small" , "run_type": "default", "cluster_specs": {}, "dependencies": [], "pre_tests": [], "post_tests": []},
                {"dataset_id": "anonymized_unic_participant_index_registre_cardiopathie_bebe"                        , "cluster_type": "xsmall", "run_type": "default", "cluster_specs": {}, "dependencies": [], "pre_tests": [], "post_tests": []},
                {"dataset_id": "anonymized_unic_participant_index_registre_cardiopathie_maman"                       , "cluster_type": "xsmall", "run_type": "default", "cluster_specs": {}, "dependencies": [], "pre_tests": [], "post_tests": []},
                {"dataset_id": "anonymized_unic_participant_index_resppa"                                            , "cluster_type": "xsmall", "run_type": "default", "cluster_specs": {}, "dependencies": [], "pre_tests": [], "post_tests": []},
                {"dataset_id": "anonymized_unic_participant_index_signature"                                         , "cluster_type": "xsmall", "run_type": "default", "cluster_specs": {}, "dependencies": [], "pre_tests": [], "post_tests": []},
                {"dataset_id": "anonymized_unic_participant_index_signature_triceps"                                 , "cluster_type": "xsmall", "run_type": "default", "cluster_specs": {}, "dependencies": [], "pre_tests": [], "post_tests": []},
                {"dataset_id": "anonymized_unic_participant_index_simapp"                                            , "cluster_type": "xsmall", "run_type": "default", "cluster_specs": {}, "dependencies": [], "pre_tests": [], "post_tests": []},


            ]
    }]
}

# Add extra param
params = default_params.copy()
params.update({"dataset_id": Param("*", type="string")})

with DAG(
    dag_id="anonymized_participant_index",
    schedule_interval=config['schedule'],
    params=params,
    default_args=default_args,
    start_date=datetime(2021, 1, 1),
    concurrency=config['concurrency'],
    catchup=False,
    tags=["anonymized"],
    dagrun_timeout=timedelta(hours=config['timeout_hours']),
    is_paused_upon_creation=True
) as dag:

    def skip_task() -> str:
        return "{% if params.dataset_id == '*' or params.dataset_id == task.task_id.split('.')[1] %}{% else %}yes{% endif %}"

    setup_dag(
        dag=dag,
        dag_config=config,
        config_file=config_file,
        jar=jar,
        schema="participant_index",
        version=version,
        spark_failure_msg=spark_failure_msg,
        skip_task=skip_task()
    )
