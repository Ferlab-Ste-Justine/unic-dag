"""
Curated CSCMED Jobs DAG
"""
# pylint: disable=duplicate-code
from datetime import datetime, timedelta

import pendulum
from airflow import DAG

from lib.config import CONFIG_FILE, JAR, SPARK_FAILURE_MSG, DEFAULT_ARGS, DEFAULT_PARAMS
from lib.slack import Slack
from tasks import create_tasks

DOC = """
# Curated CscMed Jobs DAG

ETL curated et anonymized pour les tables jobs et jobs_sections de CscMed.

### Description
Ce DAG traite exclusivement les deux tables chargées lors de la première batch de chargement de CscMed par Talend :
- **curated_cscmed_jobs_sections** (zone rouge, curated)
- **anonymized_cscmed_jobs** (zone jaune, anonymized)

Les autres tables CscMed (cliniques, quickform, etc.) sont traitées par le DAG `curated_cscmed`.

### Horaire
* __Date de début__ - 12 mars 2026
* __Jour et heure__ - Jeudi, 13h heure de Montréal
* __Intervalle__ - Chaque 4 semaines
"""

dag_config = {
    "steps": [
        {
            "destination_zone": "red",
            "destination_subzone": "curated",
            "main_class": "bio.ferlab.ui.etl.red.curated.Main",
            "multiple_main_methods": True,
            "pre_tests": [{"name": "greater_or_equal_partition_counts", "destinations": ["curated_cscmed_jobs_sections"], "cluster_type": "small"}],
            "datasets": [
                {"dataset_id": "curated_cscmed_jobs_sections", "cluster_type": "large", "run_type": "default", "pass_date": False, "dependencies": []}
            ],
            "optimize": [],
            "post_tests": []
        },
        {
            "destination_zone": "yellow",
            "destination_subzone": "anonymized",
            "main_class": "bio.ferlab.ui.etl.yellow.anonymized.Main",
            "multiple_main_methods": False,
            "pre_tests": [{"name": "greater_or_equal_partition_counts", "destinations": ["anonymized_cscmed_jobs"], "cluster_type": "small"}],
            "datasets": [
                {"dataset_id": "anonymized_cscmed_jobs", "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []}
            ],
            "optimize": [],
            "post_tests": [
                {"name": "lower_or_equal_null_counts", "destinations": ["anonymized_cscmed_jobs"], "cluster_type": "small"},
                {"name": "equal_counts"              , "destinations": ["anonymized_cscmed_jobs"], "cluster_type": "small"}
            ]
        }
    ]
}

args = DEFAULT_ARGS.copy()

dag = DAG(
    dag_id="curated_cscmed_jobs",
    doc_md=DOC,
    start_date=datetime(2026, 3, 12, 13, tzinfo=pendulum.timezone("America/Montreal")),
    schedule_interval=timedelta(weeks=4),
    params=DEFAULT_PARAMS,
    dagrun_timeout=timedelta(hours=4),
    default_args=args,
    concurrency=2,
    catchup=False,
    max_active_runs=1,
    tags=["curated"],
    is_paused_upon_creation=True,
    on_failure_callback=Slack.notify_dag_failure
)

with dag:
    create_tasks(
        dag=dag,
        dag_config=dag_config,
        config_file=CONFIG_FILE,
        jar=JAR,
        resource="cscmed_jobs",
        spark_failure_msg=SPARK_FAILURE_MSG
    )
