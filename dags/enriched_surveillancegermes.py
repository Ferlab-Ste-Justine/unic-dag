"""
Enriched Surveillance Germes DAG
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
# Enriched Surveillance Germes DAG

ETL enriched pour le projet Surveillance Germes. 

### Description
Cet ETL génère un rapport hebdomadaire sur les patients ayant eu un test positif pour une liste de pathogènes dans la 
dernière semaine, du dimanche au samedi. Le rapport sera livré tous les vendredis. La toute première exécution doit contenir 
les données historiques, avec un rapport par semaine, à partir du 1er novembre 2023. Après cette date, le rapport devient hebdomadaire. 
Une mise à jour sera faite et d'autres rapports s'ajouteront lorsque le chercheur aura reçu les autorisations éthiques.

### Horaire
* __Date de début__ - 10 novembre 2023
* __Date de fin__ - aucune
* __Jour et heure__ - Mardi, 6h heure de Montréal
* __Intervalle__ - Chaque semaine

### Fonctionnement
la date logique du DAG est envoyé comme argument à l'ETL enriched. L'intervalle est calculé à partir de cette date et 
correspond à la période du Dimanche au Samedi pécédent. Donc pour le premier rapport du 10 Novembre 2023, l'intervalle 
est du 29 Octobre au 4 Novembre 2023 et est calculer dans l'ETL.  À noter que dans airflow, le DAG est céduler a la semaine. 
Dans ce cas le 10 Novembre 2023 est le end_date et correspond au moment de génération du rapport.
Donc pour le premier rapport du 10 Novembre 2023, le start_date du DAG est le 3 Novembre 2023. 

La date de livraison (la date logique du DAG) est envoyée comme argument à l'ETL released. Cette date est 
utilisée comme version de la release.
"""

# Update default args
args = default_args.copy()
args.update({'trigger_rule': TriggerRule.NONE_FAILED})

dag = DAG(
    dag_id="enriched_surveillancegermes",
    doc_md=DOC,
    start_date=datetime(2023, 11, 3, 6, tzinfo=pendulum.timezone("America/Montreal")),
    schedule_interval="0 6 * * 2",  # Every tuesday at 06:00 AM
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
        ENRICHED_MAIN_CLASS = "bio.ferlab.ui.etl.yellow.enriched.sil.surveillancegermes.Main"

        def enriched_arguments(destination: str) -> List[str]:
            return [
                destination,
                "--config", "config/prod.conf",
                "--steps", "default",
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

        enriched_patient >> enriched_weekly_summary

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

    with TaskGroup(group_id="published") as published:
        FILEDATE ='{{ data_interval_end | ds | replace("-", "_") }}'

        parquet_bucket_name = parquet_to_excel.override(task_id="published_surveillancegermes_weekly_summary")(
        parquet_bucket_name='green-prd',
        parquet_dir_key='released/sil/surveillancegermes/latest/weekly_summary',
        excel_bucket_name='green-prd',
        excel_output_key=f'published/surveillancegermes/weekly_summary/weekly_summary_{FILEDATE}.xlsx',
        minio_conn_id=green_minio_conn_id
        )

    start() >> enriched >> released >> published >> end()
