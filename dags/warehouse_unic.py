"""
UNIC warehouse. Runs when the patient index dataset is refreshed.
"""
from datetime import timedelta

from airflow import DAG

from lib.config import DEFAULT_ARGS, DEFAULT_PARAMS, DEFAULT_START_DATE, CONFIG_FILE, JAR, SPARK_FAILURE_MSG
from lib.datasets import anonymized_unic_patient_index
from lib.slack import Slack
from tasks import create_tasks

TIMEOUT_HOURS = 2

CONFIG = {
    "steps": [{
        "destination_zone": "yellow",
        "destination_subzone": "warehouse",
        "main_class": "bio.ferlab.ui.etl.yellow.warehouse.Main",
        "multiple_main_methods": False,
        "datasets": [
            {"dataset_id": "warehouse_diagnostic_reference"                     , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []},
            {"dataset_id": "warehouse_emergency_department_episode"             , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
            {"dataset_id": "warehouse_emergency_department_triage"              , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []},
            {"dataset_id": "warehouse_fetal_ultrasound"                         , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
            {"dataset_id": "warehouse_hospitalisation_diagnosis"                , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
            {"dataset_id": "warehouse_intervention_reference"                   , "cluster_type": "small" , "run_type": "default", "pass_date": False, "dependencies": []},
            {"dataset_id": "warehouse_lab_results"                              , "cluster_type": "large" , "run_type": "default", "pass_date": False, "dependencies": []},
            {"dataset_id": "warehouse_maternal_ultrasound"                      , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
            {"dataset_id": "warehouse_medical_imaging"                          , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
            {"dataset_id": "warehouse_medication_administration"                , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
            {"dataset_id": "warehouse_medication_service"                       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
            {"dataset_id": "warehouse_microbiology"                             , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
            {"dataset_id": "warehouse_obstetrical_form_family_medical_history"  , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
            {"dataset_id": "warehouse_obstetrical_form_personal_medical_history", "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
            {"dataset_id": "warehouse_obstetrical_form_pregnancy_history"       , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
            {"dataset_id": "warehouse_pathology"                                , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
            {"dataset_id": "warehouse_pregnancy_index"                          , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
            {"dataset_id": "warehouse_respiratory_care"                         , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
            {"dataset_id": "warehouse_sociodemographics"                        , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
            {"dataset_id": "warehouse_sociodemographics_residence_history"      , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
            {"dataset_id": "warehouse_sociodemographics_civil_status_history"   , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
            {"dataset_id": "warehouse_sociodemographics_employement_history"    , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
            {"dataset_id": "warehouse_transfusions"                             , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
        ]
    }]
}

args = DEFAULT_ARGS.copy()
args["execution_timeout"] = timedelta(hours=3 / 4 * TIMEOUT_HOURS)

dag = DAG(
    dag_id="warehouse_unic",
    schedule=[anonymized_unic_patient_index],
    params=DEFAULT_PARAMS,
    default_args=args,
    start_date=DEFAULT_START_DATE,
    concurrency=3,
    catchup=False,
    max_active_runs=1,
    tags=["warehouse"],
    dagrun_timeout=timedelta(hours=TIMEOUT_HOURS),
    is_paused_upon_creation=True,
    on_failure_callback=Slack.notify_dag_failure
)

with dag:
    create_tasks(dag=dag, dag_config=CONFIG, config_file=CONFIG_FILE, jar=JAR, resource="unic",
                 spark_failure_msg=SPARK_FAILURE_MSG)
