"""
DAG manuel pour extraire les résultats des rapports de CGH oncologique des messages HL7 de Softpath
"""
# pylint: disable=invalid-name
from datetime import datetime, timedelta
from typing import List

from airflow import DAG
from airflow.models import Param

from lib.config import DEFAULT_PARAMS, DEFAULT_ARGS, SPARK_FAILURE_MSG, JAR, CONFIG_FILE, LOCAL_TZ
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.notify import end, start

DOC = """
# Curated Softpath HL7 CGH DAG

DAG manuel (aucun horaire) qui extrait les résultats des rapports de CGH sur micropuce oncologique parmi les PDF des
messages HL7 de Softpath.

### Description
1. `curated_softpath_hl7_oru_r01_obx_cgh_onco`: une ligne par rapport de CGH oncologique avec le spécimen extrait du rapport.
2. `curated_softpath_hl7_oru_r01_obx_cgh_onco_tables` : les tables extraites par docling de ces rapports, lues depuis
l'arborescence CSV de `curated_softpath_hl7_oru_r01_obx_parsing_tables`.
3. `curated_softpath_hl7_oru_r01_obx_cgh_onco_results` : le tableau de résultats de chaque rapport dans les colonnes du
gabarit (anomalie, résultat, CNV/CN-LOH, niveau de signification clinique, spécimen, version du génome), une ligne par
ligne du tableau.

### Configuration
* `branch` : branche du JAR unic-etl à utiliser (`unic-etl-<branch>.jar`).
* `start_date` / `end_date` : fenêtre `[start_date, end_date)` de `dte_of_message` à traiter (format `YYYY-MM-DD`).
Par défaut, tout l'historique. Seules les partitions présentes dans la fenêtre sont réécrites.
"""

CURATED_ZONE = "red"
CURATED_MAIN_CLASS = "bio.ferlab.ui.etl.red.curated.hl7.cgh.Main"

params = DEFAULT_PARAMS.copy()
params.update({
    "start_date": Param("1999-01-01", type="string", description="Inclusive dte_of_message start (YYYY-MM-DD)"),
    "end_date": Param("2100-01-01", type="string", description="Exclusive dte_of_message end (YYYY-MM-DD)"),
})

dag = DAG(
    dag_id="curated_softpath_hl7_cgh",
    doc_md=DOC,
    start_date=datetime(2026, 10, 1, tzinfo=LOCAL_TZ),
    schedule=None,
    params=params,
    dagrun_timeout=timedelta(hours=10),
    default_args=DEFAULT_ARGS,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=1,
    tags=["curated", "hl7"],
    on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
)

with dag:
    def get_arguments(destination: str) -> List[str]:
        """
        Generate Spark task arguments for the ETL process.
        """
        return [
            "--config", CONFIG_FILE,
            "--steps", "default",
            "--app-name", destination,
            "--destination", destination,
            "--start-date", "{{ params.start_date }}",
            "--end-date", "{{ params.end_date }}"
        ]

    cgh_tasks_config = [
        ("curated_softpath_hl7_oru_r01_obx_cgh_onco", "medium-etl"),
        ("curated_softpath_hl7_oru_r01_obx_cgh_onco_tables", "small-etl"),
        ("curated_softpath_hl7_oru_r01_obx_cgh_onco_results", "small-etl")
    ]

    cgh_tasks = [SparkOperator(
        task_id=task_name,
        name=task_name.replace("_", "-"),
        arguments=get_arguments(task_name),
        zone=CURATED_ZONE,
        spark_class=CURATED_MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=SPARK_FAILURE_MSG,
        spark_config=cluster_size,
        dag=dag
    ) for task_name, cluster_size in cgh_tasks_config]

    # each task reads what the previous one wrote
    start("start_curated_softpath_hl7_cgh") >> cgh_tasks[0] >> cgh_tasks[1] >> cgh_tasks[2] >> end("end_curated_softpath_hl7_cgh")
