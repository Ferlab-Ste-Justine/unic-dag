"""
Génération des DAGs pour le chargement dans le Catalogue des métadonnées se trouvant dans des fichiers Excel.
Un DAG par environnement postgres est généré.
"""
# pylint: disable=missing-function-docstring, invalid-name, expression-not-assigned

from datetime import datetime
from typing import List, Optional, Union

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.exceptions import AirflowFailException
from airflow.models import Param, DagRun
from airflow.utils.trigger_rule import TriggerRule

from lib.config import JAR, SPARK_FAILURE_MSG, YELLOW_MINIO_CONN_ID, DEFAULT_ARGS
from lib.operators.spark import SparkOperator
from lib.operators.upsert_csv_to_postgres import UpsertCsvToPostgres
from lib.postgres import skip_task, POSTGRES_VLAN2_CA_PATH, POSTGRES_CA_FILENAME, \
    POSTGRES_VLAN2_CA_CERT, PostgresEnv, unic_postgres_vlan2_conn_id
from lib.slack import Slack
from lib.tasks.excel import excel_to_csv
from lib.tasks.notify import start, end

ZONE = "yellow"
MAIN_CLASS = "bio.ferlab.ui.etl.catalog.csv.Main"
YELLOW_BUCKET = "yellow-prd"
table_name_list = ["resource", "analyst", "dict_table", "value_set", "value_set_code", "variable", "mapping"]
env_name = None
conn_id = None


def get_project() -> str:
    return "{{ params.project or '' }}"


def arguments(table_name: str, app_name: str, project_name: Optional[str] = None) -> \
        List[str]:
    args = [
        table_name,
        "--config", "config/prod.conf",
        "--steps", "default",
        "--app-name", app_name,
        "--env", env_name
    ]
    if project_name is not None:
        args.append("--project")
        args.append(project_name)

    return args


# Update default args
dag_args = DEFAULT_ARGS.copy()
dag_args.update({
    'trigger_rule': TriggerRule.NONE_FAILED,
    'on_failure_callback': Slack.notify_task_failure})

for env in PostgresEnv:
    env_name = env.value
    conn_id = unic_postgres_vlan2_conn_id(env)

    doc = f"""
    # Postgres **{env_name}** Load Metadata from Excel DAG
    
    DAG pour le chargement dans le Catalogue **{env_name}** des métadonnées se trouvant dans des fichiers Excel.
    
    ### Description
    Ce DAG convertit les fichiers Excel en fichier CSV pour qu'ils puissent être traités par Spark. Ensuite, les tables
    passées en paramètre au DAG seront chargées dans la BD Postgres du Catalogue dans l'environnement **{env_name}**.
    
    ### Configuration
    * Paramètre `branch` : Branche du jar à utiliser.
    * Paramètre `tables` : Liste des tables à créer dans la base de données. Par défaut, crée toutes les tables.
    * Paramètre `project` : Nom du projet à charger. Obligatoire si `dict_table`, `value_set`, `value_set_code`, `variable` ou `mapping` font partie des tables sélectionnées.
    
    ## Tables à charger
    * analyst : Charge la table `analyst`.
    * resource : Charge la table `resource`.
    * dict_table : Charge la table `dict_table`.
    * value_set : Charge la table `value_set`.
    * value_set_code : Charge la table `value_set_code`.
    * variable : Charge la table `variable`.
    * mapping : Charge la table `mapping`.
    """

    with DAG(
            dag_id=f"postgres_{env_name}_catalog_load_metadata_from_excel",
            params={
                "branch": Param("master", type="string"),
                "tables": Param(default=table_name_list, type=["array"], examples=table_name_list,
                                description="Tables to load."),
                "project": Param(None, type=["null", "string"],
                                 description="Required if 'dict_table', 'value_set', 'value_set_code', 'variable' or 'mapping' are selected in 'tables' param."),
            },
            default_args=dag_args,
            doc_md=doc,
            start_date=datetime(2024, 6, 12),
            is_paused_upon_creation=False,
            schedule_interval=None,
            tags=["postgresql"],
            on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
    ) as dag:
        @task(task_id="get_tables")
        def get_tables(ti=None) -> List[str]:
            dag_run: DagRun = ti.dag_run
            return dag_run.conf["tables"]


        @task(task_id="validate_project_param")
        def validate_project_param(tables: List[str], project: str):
            if any(table in tables for table in
                   ["dict_table", "value_set", "value_set_code", "variable", "mapping"]) and project == "":
                raise AirflowFailException(
                    "DAG param 'project' is required when tables other than 'resource' and 'analyst' are selected.")


        @task_group(group_id="excel_to_csv")
        def excel_to_csv_group():
            def excel_to_csv_task(table_name: str, s3_source_key: Optional[str] = None,
                                  s3_destination_key: Optional[str] = None, sheet_name: Union[str, int] = 0):
                return excel_to_csv.override(task_id=f"excel_to_csv_{table_name}")(
                    s3_source_bucket=YELLOW_BUCKET,
                    s3_source_key=s3_source_key,
                    s3_destination_bucket=YELLOW_BUCKET,
                    s3_destination_key=s3_destination_key,
                    s3_conn_id=YELLOW_MINIO_CONN_ID,
                    sheet_name=sheet_name,
                    skip=skip_task(table_name)
                )

            excel_to_csv_task("analyst",
                              s3_source_key="catalog/analyst.xlsx",
                              s3_destination_key=f"catalog/{env_name}/csv/output/analyst.csv")  # Directly put analyst in output directory since no transformations have to be run by the ETL
            excel_to_csv_task("resource",
                              s3_source_key="catalog/resource.xlsx",
                              s3_destination_key=f"catalog/{env_name}/csv/input/resource.csv")
            excel_to_csv_task("dict_table",
                              s3_source_key=f"catalog/dictionary_{get_project()}.xlsx",
                              s3_destination_key=f"catalog/{env_name}/csv/input/{get_project()}/dict_table.csv",
                              sheet_name="dict_table")
            excel_to_csv_task("variable",
                              s3_source_key=f"catalog/dictionary_{get_project()}.xlsx",
                              s3_destination_key=f"catalog/{env_name}/csv/input/{get_project()}/variable.csv",
                              sheet_name="variable")
            excel_to_csv_task("value_set",
                              s3_source_key=f"catalog/dictionary_{get_project()}.xlsx",
                              s3_destination_key=f"catalog/{env_name}/csv/input/{get_project()}/value_set.csv",
                              sheet_name="value_set")
            excel_to_csv_task("value_set_code",
                              s3_source_key=f"catalog/dictionary_{get_project()}.xlsx",
                              s3_destination_key=f"catalog/{env_name}/csv/input/{get_project()}/value_set_code.csv",
                              sheet_name="value_set_code")
            excel_to_csv_task("mapping",
                              s3_source_key=f"catalog/dictionary_{get_project()}.xlsx",
                              s3_destination_key=f"catalog/{env_name}/csv/input/{get_project()}/mapping.csv",
                              sheet_name="mapping")


        @task_group(group_id="load_tables")
        def load_tables_group():
            def prepare_table(table_name: str, cluster_size: str, project_name: Optional[str] = None) -> SparkOperator:
                if project_name:
                    args = arguments(table_name, app_name=f"prepare_{table_name}_table_for_{project_name}",
                                     project_name=project_name)

                else:
                    args = arguments(table_name, app_name=f"prepare_{table_name}_table")

                return SparkOperator(
                    task_id=f"prepare_{table_name}_table",
                    name=f"prepare-{table_name}-table",
                    arguments=args,
                    zone=ZONE,
                    spark_class=MAIN_CLASS,
                    spark_jar=JAR,
                    spark_failure_msg=SPARK_FAILURE_MSG,
                    spark_config=cluster_size,
                    skip=skip_task(table_name),
                )

            def load_table(table_name: str, s3_key: str, primary_keys: List[str]):
                return UpsertCsvToPostgres(
                    task_id=f"load_{table_name}_table",
                    s3_bucket="yellow-prd",
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

            # analyst table (directly loaded since no transformations have to be run by the ETL)
            load_analyst_table_task = load_table("analyst",
                                                 s3_key=f"catalog/{env_name}/csv/output/analyst/analyst.csv",
                                                 primary_keys=["name"])

            # resource table
            prepare_resource_table_task = prepare_table("resource", cluster_size="xsmall-etl")
            load_resource_table_task = load_table("resource",
                                                  s3_key=f"catalog/{env_name}/csv/output/resource/resource.csv",
                                                  primary_keys=["code"])

            prepare_resource_table_task >> load_resource_table_task
            load_analyst_table_task >> prepare_resource_table_task

            @task_group(group_id="load_project_tables")
            def load_project_tables():
                # value_set table
                prepare_value_set_table_task = prepare_table("value_set", cluster_size="xsmall-etl",
                                                             project_name=get_project())
                load_value_set_table_task = load_table("value_set",
                                                       s3_key=f"catalog/{env_name}/csv/output/{get_project()}/value_set/value_set.csv",
                                                       primary_keys=["name"])

                # dict_table table
                prepare_dict_table_table_task = prepare_table("dict_table", cluster_size="xsmall-etl",
                                                              project_name=get_project())
                load_dict_table_table_task = load_table("dict_table",
                                                        s3_key=f"catalog/{env_name}/csv/output/{get_project()}/dict_table/dict_table.csv",
                                                        primary_keys=["resource_id", "name"])

                # value_set_code table
                prepare_value_set_code_table_task = prepare_table("value_set_code", cluster_size="small-etl",
                                                                  project_name=get_project())
                load_value_set_code_table_task = load_table("value_set_code",
                                                            s3_key=f"catalog/{env_name}/csv/output/{get_project()}/value_set_code/value_set_code.csv",
                                                            primary_keys=["value_set_id", "code"])

                prepare_variable_table_task = prepare_table("variable", cluster_size="small-etl",
                                                            project_name=get_project())
                load_variable_table_task = load_table("variable",
                                                      s3_key=f"catalog/{env_name}/csv/output/{get_project()}/variable/variable.csv",
                                                      primary_keys=["path"])

                # mapping table
                prepare_mapping_table_task = prepare_table("mapping", cluster_size="small-etl",
                                                           project_name=get_project())
                load_mapping_table_task = load_table("mapping",
                                                     s3_key=f"catalog/{env_name}/csv/output/{get_project()}/mapping/mapping.csv",
                                                     primary_keys=["value_set_code_id", "original_value"])

                prepare_value_set_table_task >> load_value_set_table_task
                prepare_dict_table_table_task >> load_dict_table_table_task
                prepare_value_set_code_table_task >> load_value_set_code_table_task
                prepare_variable_table_task >> load_variable_table_task
                prepare_mapping_table_task >> load_mapping_table_task

                load_resource_table_task >> prepare_dict_table_table_task
                load_value_set_table_task >> prepare_value_set_code_table_task
                load_value_set_code_table_task >> prepare_mapping_table_task
                [load_dict_table_table_task, load_value_set_table_task] >> prepare_variable_table_task

            load_resource_table_task >> load_project_tables()


        get_tables_task = get_tables()
        start() >> get_tables_task >> validate_project_param(tables=get_tables_task, project=get_project()) \
        >> excel_to_csv_group() \
        >> load_tables_group() \
        >> end()
