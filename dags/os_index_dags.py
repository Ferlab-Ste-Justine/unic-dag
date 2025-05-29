"""
Génération des DAGs pour la préparation et le load des indexs OpenSearch.
Un DAG par environnement postgres est généré.
"""
# pylint: disable=missing-function-docstring, invalid-name, expression-not-assigned, cell-var-from-loop, syntax-error, redefined-outer-name, duplicate-code

from datetime import datetime

from airflow import DAG
from airflow.models import Param
from airflow.utils.trigger_rule import TriggerRule

from lib.groups.publish.index import index_opensearch
from lib.opensearch import OpensearchEnv
from lib.postgres import PostgresEnv
from lib.slack import Slack
from lib.tasks.notify import start, end

# Update default args
args = {
    "owner": "unic",
    "depends_on_past": False,
    'trigger_rule': TriggerRule.NONE_FAILED
}

for os_env in OpensearchEnv:
    os_env_name: str = os_env.value
    pg_env_name: str = PostgresEnv.PROD.value  # Use PROD data for all environments
    # pg_env_name: str = os_env_pg_env_mapping[os_env].value

    doc = f"""
    # Prepare and Load {pg_env_name} Index into OpenSeach {os_env_name} 
    
    DAG pour la préparation et le load des index PROD dans OpenSearch {os_env_name}.
    
    ### Description
    Ce DAG prépare et load les index PROD dans OpenSearch {os_env_name}.
    
    ## Index à Loader
    * resource centric
    * table centric 
    * variable centric 
    """

    with DAG(
            dag_id=f"os_{os_env_name}_index",
            params={
                "branch": Param("master", type="string"),
                "release_id": Param("", type=["null", "string"])
            },
            default_args=args,
            doc_md=doc,
            start_date=datetime(2024, 11, 19),
            is_paused_upon_creation=False,
            schedule_interval=None,
            tags=["opensearch"],
            on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
    ) as dag:

        start("start_os_index") >> index_opensearch(pg_env_name=pg_env_name, os_env_name=os_env_name, dag=dag) >> end("end_os_index")
