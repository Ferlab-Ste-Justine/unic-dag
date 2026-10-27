"""
Enriched Maize DAG
"""
from datetime import timedelta

import pendulum
from airflow import DAG

from lib.config import DEFAULT_ARGS, DEFAULT_PARAMS, LOCAL_TZ, CONFIG_FILE, JAR, SPARK_FAILURE_MSG
from lib.sensors.external_task import wait_for
from lib.slack import Slack
from tasks import create_tasks

DOC = """
# Enriched Maize DAG

ETL enriched pour le projet Maize.

### Description
Cet ETL génère un rapport mensuel des résultats de laboratoire de rougeole (ROUG*) des patients du projet Maize,
à partir de Softlab et de la table `warehouse_lab_results`.

### Horaire
* __Date de début__ - 3 novembre 2026
* __Date de fin__ - aucune
* __Jour et heure__ - Premier mardi du mois, 8h heure de Montréal
* __Intervalle__ - Chaque mois

### Fonctionnement
La fin de l'intervalle (date du rapport) est envoyée comme argument `--date` à l'ETL enriched, qui garde les
résultats dont la date de prélèvement est dans le mois civil précédent. Donc pour le premier rapport du 3 novembre
2026, les résultats d'octobre 2026 sont livrés.

Cette date est aussi envoyée à l'ETL released et utilisée comme version de la release. Le DAG
`unic_publish_project` est ensuite déclenché pour publier cette version dans le bucket `published-clinical-maize`.

La livraison sur SharePoint et le fichier de mapping sont faits manuellement.
"""

TIMEOUT_HOURS = 4

CONFIG = {
    "steps": [
        {
            "destination_zone": "yellow",
            "destination_subzone": "enriched",
            "main_class": "bio.ferlab.ui.etl.yellow.enriched.maize.ParticipantIndexETL",
            "multiple_main_methods": False,
            "datasets": [
                {"dataset_id": "enriched_maize_participant_index", "cluster_type": "small", "run_type": "default", "pass_date": True, "dependencies": []},
            ]
        },
        {
            "destination_zone": "green",
            "destination_subzone": "released",
            "main_class": "bio.ferlab.ui.etl.released.Main",
            "multiple_main_methods": False,
            "datasets": [
                {"dataset_id": "released_maize_participant_index", "cluster_type": "small", "run_type": "default", "pass_date": True, "dependencies": []},
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
    dag_id="enriched_maize",
    doc_md=DOC,
    schedule="0 8 * * 2#1",
    params=DEFAULT_PARAMS,
    default_args=args,
    start_date=pendulum.datetime(2026, 10, 6, tz=LOCAL_TZ),
    concurrency=1,
    catchup=False,
    max_active_runs=1,
    tags=["enriched"],
    dagrun_timeout=timedelta(hours=TIMEOUT_HOURS),
    is_paused_upon_creation=True,
    on_failure_callback=Slack.notify_dag_failure
)

with dag:
    create_tasks(dag=dag, dag_config=CONFIG, config_file=CONFIG_FILE, jar=JAR, resource="maize",
                 spark_failure_msg=SPARK_FAILURE_MSG)
    wait_for("warehouse_unic", "warehouse.warehouse_lab_results", task_id="wait_for_warehouse") \
        >> dag.get_task("enriched.start_enriched_maize")
