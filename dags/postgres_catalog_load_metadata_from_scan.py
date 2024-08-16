"""
DAG pour le chargement dans le Catalogue des métadonnées en scannant les données du lac.
"""
# pylint: disable=missing-function-docstring, invalid-name, expression-not-assigned

from datetime import datetime
from typing import List

from airflow import DAG
from airflow.decorators import task_group
from airflow.models import Param
from airflow.utils.trigger_rule import TriggerRule

from lib.config import jar, spark_failure_msg, yellow_minio_conn_id
from lib.operators.spark import SparkOperator
from lib.operators.upsert_csv_to_postgres import UpsertCsvToPostgres
from lib.postgres import skip_task, postgres_vlan2_ca_path, postgres_ca_filename, \
    postgres_vlan2_ca_cert, unic_prod_postgres_vlan2_conn_id, unic_dev_postgres_vlan2_conn_id
from lib.slack import Slack
from lib.tasks.notify import start, end

DOC = """
# Postgres Scan Metadata DAG

DAG pour le chargement dans le Catalogue des métadonnées en scannant les données du lac.

### Description
Ce DAG scan les données d'une ressource pour en déduire les métadonnées puis les charge dans le Catalogue.

### Configuration
* Paramètre `branch` : Branche du jar à utiliser.
* Paramètre `resource_code` : Code de la ressource à charger.
* Paramètre `resource_type` : Type de la ressource à charger parmi `source_system`, `research_project` ou `eqp`.
* Paramètre `to_be_published` : Si les métadonnées de la ressource doivent être publiées dans le portail.
* Paramètre `tables` : Liste des tables à créer dans la base de données. Par défaut, crée toutes les tables.
* Paramètre `env` : Environnement de la BD. Par défaut, 'prod'.

## Tables à charger
* dict_table : Charge la table `dict_table`.
* variable : Charge la table `variable`.
"""
ZONE = "yellow"
MAIN_CLASS = "bio.ferlab.ui.etl.catalog.scan.Main"
YELLOW_BUCKET = "yellow-prd"
table_name_list = ["dict_table", "variable"]

with DAG(
        dag_id="postgres_catalog_load_metadata_from_scan",
        params={
            "branch": Param("master", type="string"),
            "resource_code": Param("", type="string", description="Resource to scan."),
            "resource_type": Param("source_system", type="string", enum=["source_system", "research_project", "eqp"],
                                   description="Type of the resource."),
            "to_be_published": Param(True, type="boolean", description="Whether the resource should be published."),
            "tables": Param(default=table_name_list, type=["array"], examples=table_name_list,
                            description="Tables to load."),
            "env": Param("prod", type="string", enum=["dev", "prod"])
        },
        default_args={
            'trigger_rule': TriggerRule.NONE_FAILED,
            'on_failure_callback': Slack.notify_task_failure,
        },
        doc_md=DOC,
        start_date=datetime(2024, 7, 24),
        is_paused_upon_creation=False,
        schedule_interval=None,
        tags=["postgresql"]
) as dag:
    def get_resource_code() -> str:
        return "{{ params.resource_code }}"


    def get_resource_type() -> str:
        return "{{ params.resource_type }}"


    def get_env() -> str:
        return "{{ params.env }}"


    def get_conn_id() -> str:
        return f"{{% if params.env == 'prod' %}}{unic_prod_postgres_vlan2_conn_id}{{% else %}}{unic_dev_postgres_vlan2_conn_id}{{% endif %}}"


    def arguments(table_name: str, env: str, resource_code: str, resource_type: str, to_be_published: bool = True) -> \
    List[str]:
        return [
            table_name,
            "--config", "config/prod.conf",
            "--steps", "default",
            "--app-name", f"scan_{table_name}_table_for_{resource_code}",
            "--env", env,
            "--resource-code", resource_code,
            "--resource-type", resource_type,
            "--to-be-published", str(to_be_published)
        ]


    @task_group(group_id="load_tables")
    def load_tables_group():
        def scan_table_task(table_name: str, cluster_size: str, to_be_published: bool = True) -> SparkOperator:
            return SparkOperator(
                task_id=f"scan_{table_name}_table",
                name=f"scan-{table_name}-table",
                arguments=arguments(table_name, get_env(), get_resource_code(), get_resource_type(), to_be_published),
                zone=ZONE,
                spark_class=MAIN_CLASS,
                spark_jar=jar,
                spark_failure_msg=spark_failure_msg,
                spark_config=cluster_size,
                skip=skip_task(table_name),
                dag=dag
            )

        def load_table_task(table_name: str, s3_key: str, primary_keys: List[str]):
            return UpsertCsvToPostgres(
                task_id=f"load_{table_name}_table",
                s3_bucket=YELLOW_BUCKET,
                s3_key=s3_key,
                s3_conn_id=yellow_minio_conn_id,
                postgres_conn_id=get_conn_id(),
                postgres_ca_path=postgres_vlan2_ca_path,
                postgres_ca_filename=postgres_ca_filename,
                postgres_ca_cert=postgres_vlan2_ca_cert,
                schema_name="catalog",
                table_name=table_name,
                table_schema_path=f"sql/catalog/tables/{table_name}.sql",
                primary_keys=primary_keys,
                skip=skip_task(table_name)
            )

        scan_dict_table_task = scan_table_task("dict_table", "small-etl")
        load_dict_table_task = load_table_task("dict_table",
                                               s3_key=f"catalog/csv/output/{get_resource_code()}/dict_table/dict_table.csv",
                                               primary_keys=["resource_id", "name"])

        scan_variable_task = scan_table_task("variable", "small-etl")
        load_variable_task = load_table_task("variable",
                                             s3_key=f"catalog/csv/output/{get_resource_code()}/variable/variable.csv",
                                             primary_keys=["path"])

        scan_dict_table_task >> load_dict_table_task
        scan_variable_task >> load_variable_task
        load_dict_table_task >> scan_variable_task


    start() >> load_tables_group() >> end()
