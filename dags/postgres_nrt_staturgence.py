"""
DAG pour la création des tables dans la bd unic_datamart pour le projet Near Real-Time (NRT) StatUrgence
"""
# pylint: disable=expression-not-assigned
from datetime import datetime

from airflow import DAG
from airflow.decorators import task_group
from airflow.models import Variable, Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from core.config import postgres_ca_path, postgres_ca_filename
from core.slack import Slack
from operators.postgresca import PostgresCaOperator

DOC = """
# Postgres Near Real-Time (NRT) StatUrgence DAG

DAG pour la création des tables dans la bd unic_datamart pour le projet Near Real-Time (NRT) StatUrgence.

### Description
Ce DAG crée le schéma nrt_staturgence dans la base de données unic_datamart. Ensuite, les tables passées en
paramètre au DAG seront dropées et recréées selon les schémas définis dans `sql/nrt_staturgence`.

### Configuration
* Paramètre `tables` : Liste des tables à créer dans la base de données. Par défaut crée toutes les tables.
"""

postgres_ca_cert = Variable.get('postgres_ca_certificate', None)

# Specify path to SQL query for schema and each table
sql_config = {
    "schema": {"name": "nrt_staturgence", "postgres_schema_creation_sql_path": "sql/nrt_staturgence/schema.sql"},
    "tables": [
        {"name": "recherche_activity"            , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_activity.sql"},
        {"name": "recherche_diagnostic"          , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_diagnostic.sql"},
        {"name": "recherche_drug"                , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_drug.sql"},
        {"name": "recherche_episode"             , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_episode.sql"},
        {"name": "recherche_episode_activity"    , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_episode_activity.sql"},
        {"name": "recherche_episode_bcm"         , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_episode_bcm.sql"},
        {"name": "recherche_episode_consultation", "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_episode_consultation.sql"},
        {"name": "recherche_episode_diagnostic"  , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_episode_diagnostic.sql"},
        {"name": "recherche_episode_fadm"        , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_episode_fadm.sql"},
        {"name": "recherche_episode_location"    , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_episode_location.sql"},
        {"name": "recherche_episode_pec"         , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_episode_pec.sql"},
        {"name": "recherche_episode_spec_action" , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_episode_spec_action.sql"},
        {"name": "recherche_episode_symptom"     , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_episode_symptom.sql"},
        {"name": "recherche_episode_test"        , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_episode_test.sql"},
        {"name": "recherche_episode_transfert"   , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_episode_transfert.sql"},
        {"name": "recherche_episode_triage"      , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_episode_triage.sql"},
        {"name": "recherche_generic"             , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_generic.sql"},
        {"name": "recherche_location"            , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_location.sql"},
        {"name": "recherche_locationsector"      , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_locationsector.sql"},
        {"name": "recherche_logtransaction"      , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_logtransaction.sql"},
        {"name": "recherche_patientadt"          , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_patientadt.sql"},
        {"name": "recherche_patientadtlog"       , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_patientadtlog.sql"},
        {"name": "recherche_patientallergy"      , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_patientallergy.sql"},
        {"name": "recherche_pilotage"            , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_pilotage.sql"},
        {"name": "recherche_quest_raison_visite" , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_quest_raison_visite.sql"},
        {"name": "recherche_test"                , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_test.sql"}
    ]
}
table_name_list = [table['name'] for table in sql_config['tables']]

with DAG(
        dag_id="postgres_nrt_staturgence",
        params={"tables": Param(table_name_list, type=['array'], examples=table_name_list)},
        default_args={
            'trigger_rule': TriggerRule.NONE_FAILED,
            'on_failure_callback': Slack.notify_task_failure,
        },
        doc_md=DOC,
        start_date=datetime(2024, 5, 7),
        is_paused_upon_creation=False,
        schedule_interval=None,
        max_active_tasks=1,
        tags=["postgresql"]
) as dag:

    def skip_task(table_name: str) -> str:
        """
        Skip task if table_name is not in the list of tables passed as parameter to the DAG
        """
        return f"{{% if '{table_name}' in params.tables %}}{{% else %}}yes{{% endif %}}"

    def drop_table(table_name: str) -> str:
        """
        Generate drop table statement for the given table_name.
        """
        return f"DROP TABLE IF EXISTS {table_name};"

    start = EmptyOperator(
        task_id="start_postgres_nrt_staturgence",
        on_execute_callback=Slack.notify_dag_start
    )

    # WARNING THIS WILL DROP TABLES
    @task_group(group_id="drop_tables")
    def drop_tables():
        """
        Drop tables task group.
        """
        [PostgresCaOperator(
            task_id=f"drop_{table_config['name']}_table",
            postgres_conn_id="postgresql_bi_rw",
            sql=drop_table(table_config['name']),
            ca_path=postgres_ca_path,
            ca_filename=postgres_ca_filename,
            ca_cert=postgres_ca_cert,
            skip=skip_task(table_config['name'])
        ) for table_config in sql_config['tables']]


    @task_group(group_id="create_tables")
    def create_tables():
        """
        Create tables task group.
        """
        [PostgresCaOperator(
            task_id=f"create_{table_config['name']}_table",
            postgres_conn_id="postgresql_bi_rw",
            sql=table_config['postgres_table_creation_sql_path'],
            ca_path=postgres_ca_path,
            ca_filename=postgres_ca_filename,
            ca_cert=postgres_ca_cert,
            skip=skip_task(table_config['name'])
        ) for table_config in sql_config['tables']]


    end = EmptyOperator(
        task_id="publish_postgres_nrt_staturgence",
        on_success_callback=Slack.notify_dag_completion
    )

    start >> drop_tables() >> create_tables() >> end
