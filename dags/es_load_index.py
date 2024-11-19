"""
Génération des DAGs pour le load des indexs OpenSearch.
Un DAG par environnement postgres est généré.
"""
# pylint: disable=missing-function-docstring, invalid-name, expression-not-assigned, cell-var-from-loop

from datetime import datetime
from typing import List

from airflow import DAG
from airflow.decorators import task_group
from airflow.models import Param
from airflow.utils.trigger_rule import TriggerRule

from lib.opensearch import get_release_id, release_id
from lib.tasks.opensearch import load_index
from lib.config import default_params, jar, spark_failure_msg, es_url
from lib.postgres import PostgresEnv
from lib.slack import Slack
from lib.tasks.notify import start, end

env_name = None

def arguments(task_id: str, template_filename: str, job_type: str) -> List[str]:

    final_release_id = get_release_id(release_id(), job_type)

    return [
        task_id,
        "--config", "config/prod.conf",
        "--steps", "default",
        "--app-name", f"load_{task_id}",
        "--env", env_name,
        "--esnodes", es_url,
        "--release-id", final_release_id,
        "--template-filename", template_filename,
        "--job-type", job_type
    ]


for env in PostgresEnv:
    env_name = env.value

    doc = f"""
    # Load OpenSearch Index **{env_name}**
    
    DAG pour le load des Index OpenSearch dans l'environnement **{env_name}**.
    
    ### Description
    Ce DAG load les index OpenSearch dans l'environnement **{env_name}**. Ceci va ecentuallement faire parti du dag de publication.
    Ce dag est pour tester cet étape.
    
    ## Indexs à Loader
    * resource centric
    * table centric 
    * variable centric 
    """

    with DAG(
            dag_id=f"es_{env_name}_load_index",
            params={
                "branch": Param("master", type="string"),
                "release_id": Param(None, type="string")
            },
            default_args={
                'trigger_rule': TriggerRule.NONE_FAILED,
                'on_failure_callback': Slack.notify_task_failure,
            },
            doc_md=doc,
            start_date=datetime(2024, 11, 19),
            is_paused_upon_creation=False,
            schedule_interval=None,
            tags=["opensearch"]
    ) as dag:
        @task_group(group_id="load_indexes")
        def load_index_group():
            es_load_index_conf = [
                ("es_index_resource_centric", "resource_centric", "large-etl", "resource_centric_template.json"),
                ("es_index_table_centric", "table_centric", "large-etl", "table_centric_template.json"),
                ("es_index_variable_centric", "variable_centric", "large-etl", "variable_centric_template.json")
            ] # change release_id to None after initial load, put release_id in params in permenant ETL

            [load_index(task_id, arguments(task_id, template_filename, job_type),
                        jar, spark_failure_msg, cluster_size, dag) for
             task_id, job_type, cluster_size, template_filename in es_load_index_conf]


        start("start_es_prepare_index") >> load_index_group() >> end("end_postgres_prepare_index")
