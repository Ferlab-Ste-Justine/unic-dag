"""
Enriched SprintKid DAG. Waits for the warehouse tables it reads before running.
"""
from datetime import timedelta

import pendulum
from airflow import DAG

from lib.config import DEFAULT_ARGS, DEFAULT_PARAMS, LOCAL_TZ, CONFIG_FILE, JAR, SPARK_FAILURE_MSG
from lib.sensors.external_task import wait_for
from lib.slack import Slack
from tasks import create_tasks

TIMEOUT_HOURS = 4

CONFIG = {
    "steps": [
        {
            "destination_zone": "yellow",
            "destination_subzone": "enriched",
            "main_class": "bio.ferlab.ui.etl.yellow.enriched.sprintkid.Main",
            "multiple_main_methods": True,
            "datasets": [
                {"dataset_id": "enriched_sprintkid_hospital_data"                           , "cluster_type": "medium", "run_type": "default", "pass_date": True, "dependencies": ["enriched_sprintkid_respiratory_pathogen_diagnostics"]},
                {"dataset_id": "enriched_sprintkid_live_region_v20_import_template"         , "cluster_type": "small" , "run_type": "default", "pass_date": True, "dependencies": ["enriched_sprintkid_participant_index", "enriched_sprintkid_stream_2_aefi_screening", "enriched_sprintkid_respiratory_pathogen_diagnostics", "enriched_sprintkid_patient_data", "enriched_sprintkid_hospital_data", "enriched_sprintkid_stream_34_screening"]},
                {"dataset_id": "enriched_sprintkid_live_region_v20_import_template_balanced", "cluster_type": "medium", "run_type": "default", "pass_date": True, "dependencies": ["enriched_sprintkid_live_region_v20_import_template"]},
                {"dataset_id": "enriched_sprintkid_participant_index"                       , "cluster_type": "medium", "run_type": "default", "pass_date": True, "dependencies": ["enriched_sprintkid_stream_2_aefi_screening", "enriched_sprintkid_respiratory_pathogen_diagnostics"]},
                {"dataset_id": "enriched_sprintkid_patient_data"                            , "cluster_type": "medium", "run_type": "default", "pass_date": True, "dependencies": ["enriched_sprintkid_respiratory_pathogen_diagnostics"]},
                {"dataset_id": "enriched_sprintkid_respiratory_pathogen_diagnostics"        , "cluster_type": "medium", "run_type": "default", "pass_date": True, "dependencies": []},
                {"dataset_id": "enriched_sprintkid_stream_2_aefi_screening"                 , "cluster_type": "medium", "run_type": "default", "pass_date": True, "dependencies": []},
                {"dataset_id": "enriched_sprintkid_stream_34_screening"                     , "cluster_type": "small" , "run_type": "default", "pass_date": True, "dependencies": ["enriched_sprintkid_respiratory_pathogen_diagnostics"]},
                {"dataset_id": "enriched_surveillancegermes_patient"                        , "cluster_type": "medium", "run_type": "default", "pass_date": True, "dependencies": []},
                {"dataset_id": "enriched_surveillancegermes_weekly_summary"                 , "cluster_type": "small" , "run_type": "default", "pass_date": True, "dependencies": ["enriched_surveillancegermes_patient"]},
            ]
        },
        {
            "destination_zone": "green",
            "destination_subzone": "released",
            "main_class": "bio.ferlab.ui.etl.released.Main",
            "multiple_main_methods": False,
            "datasets": [
                {"dataset_id": "released_sprintkid_live_region_v20_import_template"         , "cluster_type": "xsmall", "run_type": "default", "pass_date": True, "dependencies": []},
                {"dataset_id": "released_sprintkid_live_region_v20_import_template_balanced", "cluster_type": "xsmall", "run_type": "default", "pass_date": True, "dependencies": []},
                {"dataset_id": "released_surveillancegermes_weekly_summary"                 , "cluster_type": "xsmall", "run_type": "default", "pass_date": True, "dependencies": []},
            ]
        },
        {
            "destination_zone": "green",
            "destination_subzone": "published",
            "pass_date": True,
            "include_dictionary": False
        }
    ]
}

args = DEFAULT_ARGS.copy()
args["execution_timeout"] = timedelta(hours=3 / 4 * TIMEOUT_HOURS)

dag = DAG(
    dag_id="enriched_sprintkid",
    schedule="0 8 * * 2",
    params=DEFAULT_PARAMS,
    default_args=args,
    start_date=pendulum.datetime(2025, 11, 17, tz=LOCAL_TZ),
    concurrency=4,
    catchup=True,
    max_active_runs=1,
    tags=["enriched"],
    dagrun_timeout=timedelta(hours=TIMEOUT_HOURS),
    is_paused_upon_creation=True,
    on_failure_callback=Slack.notify_dag_failure
)

with dag:
    create_tasks(dag=dag, dag_config=CONFIG, config_file=CONFIG_FILE, jar=JAR, resource="sprintkid",
                 spark_failure_msg=SPARK_FAILURE_MSG)
    wait_for("warehouse_unic", "warehouse.warehouse_lab_results", "warehouse.warehouse_microbiology",
             "warehouse.warehouse_sociodemographics", task_id="wait_for_warehouse") \
        >> dag.get_task("enriched.start_enriched_sprintkid")
