"""
Génération des DAGs pour le chargement dans le Catalogue des métadonnées en scannant les données du lac.
Un DAG par environnement postgres est généré.
"""
# pylint: disable=missing-function-docstring, invalid-name, expression-not-assigned

from datetime import datetime
from typing import List

from airflow import DAG
from airflow.decorators import task_group
from airflow.models import Param
from airflow.utils.trigger_rule import TriggerRule

from lib.config import JAR, SPARK_FAILURE_MSG, YELLOW_MINIO_CONN_ID, DEFAULT_ARGS
from lib.operators.spark import SparkOperator
from lib.operators.upsert_csv_to_postgres import UpsertCsvToPostgres
from lib.postgres import skip_task, POSTGRES_VLAN2_CA_PATH, POSTGRES_CA_FILENAME, \
    POSTGRES_VLAN2_CA_CERT, PostgresEnv, unic_postgres_vlan2_conn_id
from lib.slack import Slack
from lib.tasks.notify import start, end

ZONE = "yellow"
MAIN_CLASS = "bio.ferlab.ui.etl.catalog.scan.Main"
YELLOW_BUCKET = "yellow-prd"
table_name_list = ["dict_table", "variable"]
env_name = None
conn_id = None

# Update default args
args = DEFAULT_ARGS.copy()
args.update({
    'trigger_rule': TriggerRule.NONE_FAILED,
    'on_failure_callback': Slack.notify_task_failure})


def get_resource_code() -> str:
    return "{{ params.resource_code }}"


def get_resource_type() -> str:
    return "{{ params.resource_type }}"


def arguments(table_name: str, resource_code: str, resource_type: str) -> \
        List[str]:
    return [
        table_name,
        "--config", "config/prod.conf",
        "--steps", "default",
        "--app-name", f"scan_{table_name}_table_for_{resource_code}",
        "--env", env_name,
        "--resource-code", resource_code,
        "--resource-type", resource_type
    ]


for env in PostgresEnv:
    env_name = env.value
    conn_id = unic_postgres_vlan2_conn_id(env)

    doc = f"""
    # Postgres **{env_name}** Scan Metadata DAG
    
    DAG pour le chargement dans le Catalogue **{env_name}** des métadonnées en scannant les données du lac.
    
    ### Description
    Ce DAG scanne les données d'une ressource pour en extraire les métadonnées, puis les charge dans le Catalogue **{env_name}**.
    > ⚠️ **Attention**  
    > Ce DAG écrasera les métadonnées existantes dans le catalogue si la ressource existe déjà.
    
    #### Données scannées
    Le DAG scanne les DatasetConf et les données dans Minio des zones suivantes :
    
    - `source_system`: anonymized  
    - `research_project`: released (latest)  
    - `eqp`: released (latest)
    
    #### Données chargées
    Les colonnes suivantes sont chargées dans le catalogue pour la table `dict_table` :
    - name
    - resource_id
    
    Les colonnes suivantes sont chargées dans le catalogue pour la table `variable` :
    - name
    - path
    - value_type
    - table_id
    - rolling_version
    
    ### Configuration
    * Paramètre `branch` : Branche du jar à utiliser.
    * Paramètre `resource_code` : Code de la ressource à charger.
    * Paramètre `resource_type` : Type de la ressource à charger parmi `source_system`, `research_project` ou `eqp`.
    * Paramètre `tables` : Liste des tables postgres dans lesquelles charger les métadonnées parmi `dict_table` et `variable`. Par défaut, les deux tables sont chargées.
    """

    with DAG(
            dag_id=f"postgres_{env_name}_catalog_load_metadata_from_scan",
            params={
                "branch": Param("master", type="string"),
                "resource_code": Param("", type="string", description="Resource to scan."),
                "resource_type": Param("source_system", type="string",
                                       enum=["source_system", "research_project", "eqp"],
                                       description="Type of the resource."),
                "tables": Param(default=table_name_list, type=["array"], examples=table_name_list,
                                description="Tables to load."),
            },
            default_args=args,
            doc_md=doc,
            start_date=datetime(2024, 7, 24),
            is_paused_upon_creation=False,
            schedule_interval=None,
            tags=["postgresql"],
            on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
    ) as dag:
        @task_group(group_id="load_tables")
        def load_tables_group():
            def scan_table_task(table_name: str, cluster_size: str) -> SparkOperator:
                return SparkOperator(
                    task_id=f"scan_{table_name}_table",
                    name=f"scan-{table_name}-table",
                    arguments=arguments(table_name, get_resource_code(), get_resource_type()),
                    zone=ZONE,
                    spark_class=MAIN_CLASS,
                    spark_jar=JAR,
                    spark_failure_msg=SPARK_FAILURE_MSG,
                    spark_config=cluster_size,
                    skip=skip_task(table_name),
                )

            def load_table_task(table_name: str, s3_key: str, primary_keys: List[str]):
                return UpsertCsvToPostgres(
                    task_id=f"load_{table_name}_table",
                    s3_bucket=YELLOW_BUCKET,
                    s3_key=s3_key,
                    s3_conn_id=YELLOW_MINIO_CONN_ID,
                    postgres_conn_id=conn_id,
                    postgres_ca_path=POSTGRES_VLAN2_CA_PATH,
                    postgres_ca_filename=POSTGRES_CA_FILENAME,
                    postgres_ca_cert=POSTGRES_VLAN2_CA_CERT,
                    schema_name="catalog",
                    table_name=table_name,
                    table_schema_path=f"sql/catalog/tables/{table_name}.sql",
                    primary_keys=primary_keys,
                    excluded_columns=["created_at"],
                    skip=skip_task(table_name)
                )

            scan_dict_table_task = scan_table_task("dict_table", "small-etl")
            load_dict_table_task = load_table_task("dict_table",
                                                   s3_key=f"catalog/{env_name}/csv/output/{get_resource_code()}/dict_table/dict_table.csv",
                                                   primary_keys=["resource_id", "name"])

            scan_variable_task = scan_table_task("variable", "small-etl")
            load_variable_task = load_table_task("variable",
                                                 s3_key=f"catalog/{env_name}/csv/output/{get_resource_code()}/variable/variable.csv",
                                                 primary_keys=["path"])

            scan_dict_table_task >> load_dict_table_task
            scan_variable_task >> load_variable_task
            load_dict_table_task >> scan_variable_task


        start() >> load_tables_group() >> end()
