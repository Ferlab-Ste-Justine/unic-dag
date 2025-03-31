"""
Enriched SRINT KID DAG
"""
# pylint: disable=missing-function-docstring, duplicate-code, expression-not-assigned, invalid-name

from datetime import datetime, timedelta
from typing import List

import pendulum
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from lib.config import default_params, default_timeout_hours, default_args, spark_failure_msg
from lib.config import green_minio_conn_id
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.excel import parquet_to_excel
from lib.tasks.notify import start, end

JAR = 's3a://spark-prd/jars/unic-etl-{{ params.branch }}.jar'

DOC = """
# Enriched SPRINT KID AND Surveillance Germes DAG

ETL enriched pour le projet SPRINT KID/Surveillance Germes. 

### Description
Cet ETL génère un rapport hebdomadaire sur les patients ayant eu un test positif pour une liste de pathogènes ou un 
diagnostic d'événement non désirable après la vaccination dans la dernière semaine, du dimanche au samedi. Il regroupe
les deux projets Sprint-Kid et Germes, qui sont en pratique, le même projet.

Le rapport sera livré tous les mardi le plus tôt possible. 

### Horaire
* __Date de début__ - 6 janvier 2025
* __Date de fin__ - aucune
* __Jour et heure__ - mardi, 6h heure de Montréal
* __Intervalle__ - chaque semaine

### Fonctionnement
La date logique du DAG est envoyée comme argument à l'ETL enriched. L'intervalle est calculé à partir de cette date et 
correspond à la période du dimanche au samedi précédent. Donc pour le premier rapport du 6 janvier 2025, l'intervalle 
est du 29 décembre 2024 au 4 janvier 2025 et est calculé dans l'ETL. 
Dans ce cas le 6 janvier 2025 est le end_date et correspond au moment de génération du rapport.

La date de livraison (la date logique du DAG) est envoyée comme argument à l'ETL released. Cette date est 
utilisée comme version de la release.

La livraison doit inclure les tables weekly_summary (germes) et stream_2_aefi_screening (sprint-kid) de la zone verte. 
La table participant_index (sprint-kid) de la zone enriched doit être désanonymisée entièrement et livrée dans la zone info nominative.

### PREREQUIS
Les tables du lac suivantes doivent être mises à jour et avoir les données pour la période couverte avant chaque exécution 
de ce DAG
* anonymized_clinibaseci_sejhosp_tb
* anonymized_eclinibase_v_identification
* anonymized_eclinibase_v_identification_adresse
* anonymized_staturgence_recherche_episode
* anonymized_staturgence_recherche_episode_diagnostic
* anonymized_staturgence_recherche_episode_triage
* anonymized_unic_patient_index
* warehouse_lab_results (et ses dépendances)
* warehouse_microbiology (et ses dépendances)
* warehouse_sociodemographics (et ses dépendances)
"""

# Update default args
args = default_args.copy()
args.update({'trigger_rule': TriggerRule.NONE_FAILED})

dag = DAG(
    dag_id="enriched_sprintkid",
    doc_md=DOC,
    start_date=datetime(2024, 12, 29, 6, tzinfo=pendulum.timezone("America/Montreal")),
    schedule_interval="0 6 * * 2",  # Every tuesday at 6:00 AM
    params=default_params,
    dagrun_timeout=timedelta(hours=default_timeout_hours),
    default_args=args,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["enriched"],
    on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
)

with dag:

    with TaskGroup(group_id="enriched") as enriched:
        ENRICHED_ZONE = "yellow"
        ENRICHED_MAIN_CLASS = "bio.ferlab.ui.etl.yellow.enriched.sprintkid.Main"

        def enriched_arguments(destination: str, run_type: str = "default") -> List[str]:
            return [
                destination,
                "--config", "config/prod.conf",
                "--steps", run_type,
                "--app-name", destination,
                "--date", "{{ data_interval_end | ds }}"
            ]


        enriched_patient = SparkOperator(
            task_id="enriched_surveillancegermes_patient",
            name="enriched-surveillancegermes-patient",
            arguments=enriched_arguments("enriched_surveillancegermes_patient"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_weekly_summary = SparkOperator(
            task_id="enriched_surveillancegermes_weekly_summary",
            name="enriched-surveillancegermes-weekly-summary",
            arguments=enriched_arguments("enriched_surveillancegermes_weekly_summary"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag,
        )


        enriched_respiratory_pathogen_diagnostics = SparkOperator(
            task_id="enriched_sprintkid_respiratory_pathogen_diagnostics",
            name="enriched-sprintkid-respiratory-pathogen-diagnostics",
            arguments=enriched_arguments("enriched_sprintkid_respiratory_pathogen_diagnostics", "default"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag
        )

        enriched_stream_2_aefi_screening = SparkOperator(
            task_id="enriched_sprintkid_stream_2_aefi_screening",
            name="enriched-sprintkid-stream-2-aefi-screening",
            arguments=enriched_arguments("enriched_sprintkid_stream_2_aefi_screening", "default"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag,
        )

        enriched_participant_index = SparkOperator(
            task_id="enriched_sprintkid_participant_index",
            name="enriched-sprintkid-participant-index",
            arguments=enriched_arguments("enriched_sprintkid_participant_index", "default"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag,
        )

        enriched_patient_data = SparkOperator(
            task_id="enriched_sprintkid_patient_data",
            name="enriched-sprintkid-patient-data",
            arguments=enriched_arguments("enriched_sprintkid_patient_data", "default"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag,
        )

        enriched_hospital_data = SparkOperator(
            task_id="enriched_sprintkid_hospital_data",
            name="enriched-sprintkid-hospital-data",
            arguments=enriched_arguments("enriched_sprintkid_hospital_data", "default"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag,
        )

        enriched_live_region_v20_import_template = SparkOperator(
            task_id="enriched_sprintkid_live_region_v20_import_template",
            name="enriched-sprintkid-live-region-v20-import-template",
            arguments=enriched_arguments("enriched_sprintkid_live_region_v20_import_template", "default"),
            zone=ENRICHED_ZONE,
            spark_class=ENRICHED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag,
        )

        enriched_patient >> enriched_weekly_summary
        enriched_respiratory_pathogen_diagnostics >> [enriched_patient_data, enriched_hospital_data]
        enriched_stream_2_aefi_screening >> [enriched_patient_data, enriched_hospital_data]
        [enriched_patient_data, enriched_hospital_data] >> enriched_participant_index >> enriched_live_region_v20_import_template

    with TaskGroup(group_id="released") as released:
        RELEASED_ZONE = "green"
        RELEASED_MAIN_CLASS = "bio.ferlab.ui.etl.green.released.Main"

        def released_arguments(destination: str) -> List[str]:
            # {{ ds }} is the DAG run’s logical date as YYYY-MM-DD. This date is used as the released version.
            return [
                "--config", "config/prod.conf",
                "--steps", "default",
                "--app-name", destination,
                "--destination", destination,
                "--version", "{{ data_interval_end | ds }}"
            ]


        released_weekly_summary = SparkOperator(
            task_id="released_surveillancegermes_weekly_summary",
            name="released-surveillancegermes-weekly-summary",
            arguments=released_arguments("released_surveillancegermes_weekly_summary"),
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag,
        )

        released_sprintkid_live_region_v20_import_template = SparkOperator(
            task_id="released_sprintkid_live_region_v20_import_template",
            name="released-sprintkid-live-region-v20-import-template",
            arguments=released_arguments("released_sprintkid_live_region_v20_import_template"),
            zone=RELEASED_ZONE,
            spark_class=RELEASED_MAIN_CLASS,
            spark_jar=JAR,
            spark_failure_msg=spark_failure_msg,
            spark_config="medium-etl",
            dag=dag,
        )

    with TaskGroup(group_id="published") as published:

        FILEDATE ='{{ data_interval_end | ds | replace("-", "_") }}'

        parquet_bucket_name= parquet_to_excel.override(task_id="published_sprintkid_live_region_v20_import_template")(
            parquet_bucket_name='green-prd',
            parquet_dir_key='released/sprintkid/latest/live_region_v20_import_template',
            excel_bucket_name='green-prd',
            excel_output_key=f'published/sprintkid/live_region_v20_import_template/live_region_v20_import_template_{FILEDATE}.xlsx',
            minio_conn_id=green_minio_conn_id
        )

        parquet_bucket_name= parquet_to_excel.override(task_id="published_surveillancegermes_weekly_summary")(
            parquet_bucket_name='green-prd',
            parquet_dir_key='released/sprintkid/latest/weekly_summary',
            excel_bucket_name='green-prd',
            excel_output_key=f'published/sprintkid/weekly_summary/weekly_summary_{FILEDATE}.xlsx',
            minio_conn_id=green_minio_conn_id
        )

    start() >> enriched >> released >> published >> end()
