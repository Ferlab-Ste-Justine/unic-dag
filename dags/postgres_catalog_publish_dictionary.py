"""
Génération des DAGs pour le publication du dictionnaire d'un projet dans le Portail de l'UNIC.
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

from lib.config import jar, spark_failure_msg, yellow_minio_conn_id, default_args, config_file
from lib.operators.spark import SparkOperator
from lib.operators.upsert_csv_to_postgres import UpsertCsvToPostgres
from lib.postgres import skip_task, postgres_vlan2_ca_path, postgres_ca_filename, \
    postgres_vlan2_ca_cert, PostgresEnv, unic_postgres_vlan2_conn_id
from lib.slack import Slack
from lib.tasks.excel import excel_to_csv
from lib.tasks.notify import start, end

ZONE = "green"
SUBZONE = "published"
MAIN_CLASS = "bio.ferlab.ui.etl.green.published.Main"
YELLOW_BUCKET = "green-prd"
table_name_list = ["resource", "analyst", "dict_table", "value_set", "value_set_code", "variable", "mapping"]
env_name = None
conn_id = None


def get_project() -> str:
    return "{{ params.project or '' }}"


def arguments(table_name: str, app_name: str, project_name: Optional[str] = None) -> List[str]:
    args = [
        "--config", config_file,
        "--steps", run_type,
        "--app-name", destination,
        "--destination", destination
    ]

    return args


# Update default args
dag_args = default_args.copy()
dag_args.update({
    'trigger_rule': TriggerRule.NONE_FAILED,
    'on_failure_callback': Slack.notify_task_failure})

for env in PostgresEnv:
    env_name = env.value
    conn_id = unic_postgres_vlan2_conn_id(env)

    doc = f"""
    # Publish table to Unic Portal
    
    DAG pour la publication d'un projet dans le portail de l'UNIC dans l'environnement **{env_name}**.
    
    ### Description
    Ce DAG publie le dictionaire d'un projet dans la zone published de minio et inclue le projet dans les indexs 
    opensearch du portail Unic. Il a un dag par environnment postgres.
    
    ### Configuration
    * Paramètre `branch` : Branche du jar à utiliser.
    * Paramètre `resource_code` : Code de la ressource à charger.
    * Paramètre `dict_version` : version du dictionnaire à publier dans published.
    * Paramètre `versions_to_publish` : listes de Date Versions dans released à publier dans published.

    """

    with DAG(
            dag_id=f"postgres_{env_name}_catalog_load_metadata_from_excel",
            params={
                "branch": Param("master", type="string"),
                "resource_code": Param("", type="string", description="Resource to publish."),
                "dict_version": Param("", type="string", description="Version of dictionary. Use semantic versioning. Ex: v1.0.0"),
                "versions_to_publish": Param(default=[], type=["array"], description="List of Dates to publish. Ex: 2001-01-01"),
            },
            default_args=dag_args,
            doc_md=doc,
            start_date=datetime(2024, 12, 16),
            is_paused_upon_creation=False,
            schedule_interval=None,
            tags=["postgresql", "published", "opensearch"],
            on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
    ) as dag:
        @task(task_id='get_dict_current_version')
        def get_dict_current_version(postgres_resource: PostgresResource, sql_config: dict, conn_id: str = conn_id,
                                     ca_path: str = postgres_vlan2_ca_path, ca_cert: str = postgres_vlan2_ca_cert) -> string:

             return PostgresCaOperator(
                task_id=f"create_{sql_config[postgres_resource.value]['name']}_{postgres_resource.value}",
                postgres_conn_id=conn_id,
                sql=sql_config[postgres_resource.value][f'postgres_{postgres_resource.value}_creation_sql_path'],
                ca_path=ca_path,
                ca_filename=postgres_ca_filename,
                ca_cert=ca_cert
            )


        @task_group(group_id="excel_to_csv")
        def excel_to_csv_group():
            def excel_to_csv_task(table_name: str, s3_source_key: Optional[str] = None,
                                  s3_destination_key: Optional[str] = None, sheet_name: Union[str, int] = 0):
                return excel_to_csv.override(task_id=f"excel_to_csv_{table_name}")(
                    s3_source_bucket=YELLOW_BUCKET,
                    s3_source_key=s3_source_key,
                    s3_destination_bucket=YELLOW_BUCKET,
                    s3_destination_key=s3_destination_key,
                    s3_conn_id=yellow_minio_conn_id,
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
                    spark_jar=jar,
                    spark_failure_msg=spark_failure_msg,
                    spark_config=cluster_size,
                    skip=skip_task(table_name),
                )

            def load_table(table_name: str, s3_key: str, primary_keys: List[str]):
                return UpsertCsvToPostgres(
                    task_id=f"load_{table_name}_table",
                    s3_bucket="yellow-prd",
                    s3_key=s3_key,
                    s3_conn_id=yellow_minio_conn_id,
                    postgres_conn_id=conn_id,
                    postgres_ca_path=postgres_vlan2_ca_path,
                    postgres_ca_filename=postgres_ca_filename,
                    postgres_ca_cert=postgres_vlan2_ca_cert,
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
