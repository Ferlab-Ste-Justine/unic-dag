"""
DAG pour le chargement dans le Catalogue des métadonnées se trouvant dans des fichiers Excel.
"""
# pylint: disable=missing-function-docstring, invalid-name, expression-not-assigned

from datetime import datetime
from typing import List, Optional, Union

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.exceptions import AirflowFailException
from airflow.models import Param, DagRun
from airflow.utils.trigger_rule import TriggerRule

from lib.config import jar, spark_failure_msg
from lib.operators.spark import SparkOperator
from lib.postgres import skip_task
from lib.slack import Slack
from lib.tasks.excel import excel_to_csv
from lib.tasks.notify import start, end

DOC = """
# Postgres Load Metadata from Excel DAG

DAG pour le chargement dans le Catalogue des métadonnées se trouvant dans des fichiers Excel.

### Description
Ce DAG convertit les fichiers Excel en fichier CSV pour qu'ils puissent être traités par Spark. Ensuite, les tables
passées en paramètre au DAG seront chargées dans la BD Postgres du Catalogue.

### Configuration
* Paramètre `branch` : Branche du jar à utiliser.
* Paramètre `tables` : Liste des tables à créer dans la base de données. Par défaut, crée toutes les tables.
* Paramètre `project` : Nom du projet à charger. Obligatoire si `dictionary` fait partie des tables sélectionnées.
* Paramètre `run_type` : Type d'exécution. `default` pour faire un upsert dans la BD, `initial` pour overwrite.

## Tables à charger
* analyst : Charge la table `analyst`.
* resource : Charge la table `resource`.
* dictionary : Charge les lignes associées au projet passé en paramètre dans les tables `dictionary`, `dict_table`, 
`value_set`, `value_set_code`, `variable`, `mapping`.  
"""
ZONE = "yellow"
MAIN_CLASS = "bio.ferlab.ui.etl.catalog.csv.Main"
YELLOW_BUCKET = "yellow-prd"
table_name_list = ["resource", "analyst", "dictionary"]

with DAG(
        dag_id="postgres_catalog_load_metadata_from_excel",
        params={
            "branch": Param("master", type="string"),
            "tables": Param(default=table_name_list, type=["array"], examples=table_name_list,
                            description="Tables to load."),
            "project": Param(None, type=["null", "string"],
                             description="Required if 'dictionary' is selected in 'tables' param."),
            "run_type": Param(default="default", type=["string"], enum=["default", "initial"],
                              description="Initial will reset all rows related to the project, across all tables.")
        },
        default_args={
            'trigger_rule': TriggerRule.NONE_FAILED,
            'on_failure_callback': Slack.notify_task_failure,
        },
        doc_md=DOC,
        start_date=datetime(2024, 6, 12),
        is_paused_upon_creation=False,
        schedule_interval=None,
        tags=["postgresql"]
) as dag:
    def get_project() -> str:
        return "{{ params.project or '' }}"


    def get_run_type() -> str:
        return "{{ params.run_type }}"


    def arguments(table_name: str, app_name: str, run_type: Optional[str] = None, project_name: Optional[str] = None) -> \
            List[str]:
        args = [
            table_name,
            "--config", "config/prod.conf",
            "--steps", run_type if run_type is not None else "default",
            "--app-name", app_name
        ]
        if project_name is not None:
            args.append("--project-name")
            args.append(project_name)

        return args


    @task(task_id="get_tables")
    def get_tables(ti=None) -> List[str]:
        dag_run: DagRun = ti.dag_run
        return dag_run.conf["tables"]


    @task(task_id="validate_project_param")
    def validate_project_param(tables: List[str], project: str):
        if "dictionary" in tables and project == "":
            raise AirflowFailException("DAG param 'project' is required when 'dictionary' table is selected.")


    @task_group(group_id="excel_to_csv")
    def excel_to_csv_group():
        def excel_to_csv_task(table_name: str, s3_source_key: Optional[str] = None,
                              s3_destination_key: Optional[str] = None, sheet_name: Union[str, int] = 0):
            if s3_source_key is None:
                s3_source_key = f"catalog/{table_name}.xlsx"

            if s3_destination_key is None:
                s3_destination_key = f"catalog/csv/{table_name}.csv"

            return excel_to_csv.override(task_id=f"excel_to_csv_{table_name}")(
                s3_source_bucket=YELLOW_BUCKET,
                s3_source_key=s3_source_key,
                s3_destination_bucket=YELLOW_BUCKET,
                s3_destination_key=s3_destination_key,
                sheet_name=sheet_name,
                skip=skip_task(table_name)
            )

        excel_to_csv_task("analyst")
        excel_to_csv_task("resource")
        excel_to_csv_task("dictionary",
                          s3_source_key=f"catalog/dictionary_{get_project()}.xlsx",
                          s3_destination_key=f"catalog/csv/{get_project()}/dictionary.csv",
                          sheet_name="dictionary")
        excel_to_csv_task("dict_table",
                          s3_source_key=f"catalog/dictionary_{get_project()}.xlsx",
                          s3_destination_key=f"catalog/csv/{get_project()}/dict_table.csv",
                          sheet_name="dict_table")
        excel_to_csv_task("variable",
                          s3_source_key=f"catalog/dictionary_{get_project()}.xlsx",
                          s3_destination_key=f"catalog/csv/{get_project()}/variable.csv",
                          sheet_name="variable")
        excel_to_csv_task("value_set",
                          s3_source_key=f"catalog/dictionary_{get_project()}.xlsx",
                          s3_destination_key=f"catalog/csv/{get_project()}/value_set.csv",
                          sheet_name="value_set")
        excel_to_csv_task("value_set_code",
                          s3_source_key=f"catalog/dictionary_{get_project()}.xlsx",
                          s3_destination_key=f"catalog/csv/{get_project()}/value_set_code.csv",
                          sheet_name="value_set_code")
        excel_to_csv_task("mapping",
                          s3_source_key=f"catalog/dictionary_{get_project()}.xlsx",
                          s3_destination_key=f"catalog/csv/{get_project()}/mapping.csv",
                          sheet_name="mapping")


    @task_group(group_id="load_tables")
    def load_tables_group():
        def load_table(table_name: str, cluster_size: str) -> SparkOperator:
            return SparkOperator(
                task_id=f"load_{table_name}_table",
                name=f"load-{table_name}-table",
                arguments=arguments(table_name, app_name=f"load_{table_name}_table"),
                zone=ZONE,
                spark_class=MAIN_CLASS,
                spark_jar=jar,
                spark_failure_msg=spark_failure_msg,
                spark_config=cluster_size,
                skip=skip_task(table_name),
                dag=dag
            )

        load_analyst_table_task = load_table("analyst", cluster_size="xsmall-etl")
        load_resource_table_task = load_table("resource", cluster_size="xsmall-etl")

        def load_project_table(table_name: str, cluster_size: str, project_name: Optional[str],
                               run_type: Optional[str] = None) -> SparkOperator:
            return SparkOperator(
                task_id=f"load_{table_name}_table",
                name=f"load-{table_name}-table",
                arguments=arguments(table_name, app_name=f"load_{table_name}_table_for_{get_project()}",
                                    run_type=run_type,
                                    project_name=project_name),
                zone=ZONE,
                spark_class=MAIN_CLASS,
                spark_jar=jar,
                spark_failure_msg=spark_failure_msg,
                spark_config=cluster_size,
                skip=skip_task(table_name),
                dag=dag
            )

        @task_group(group_id="load_project_tables")
        def load_project_tables():
            load_dictionary_table_task = load_project_table("dictionary", cluster_size="xsmall-etl",
                                                            run_type=get_run_type(), project_name=get_project())
            load_value_set_table_task = load_project_table("value_set", cluster_size="xsmall-etl",
                                                           run_type=get_run_type(),
                                                           project_name=get_project())
            load_dict_table_table_task = load_project_table("dict_table", cluster_size="xsmall-etl",
                                                            project_name=get_project())
            load_value_set_code_table_task = load_project_table("value_set_code", cluster_size="small-etl",
                                                                project_name=get_project())
            load_variable_table_task = load_project_table("variable", cluster_size="small-etl",
                                                          project_name=get_project())
            load_mapping_table_task = load_project_table("mapping", cluster_size="small-etl",
                                                         project_name=get_project())

            load_dictionary_table_task >> load_dict_table_table_task
            load_value_set_table_task >> load_value_set_code_table_task
            load_value_set_code_table_task >> load_mapping_table_task
            [load_dict_table_table_task, load_value_set_table_task] >> load_variable_table_task

        load_analyst_table_task >> load_resource_table_task >> load_project_tables()


    get_tables_task = get_tables()
    start() >> get_tables_task >> validate_project_param(tables=get_tables_task, project=get_project()) \
        >> excel_to_csv_group() \
        >> load_tables_group() \
        >> end()
