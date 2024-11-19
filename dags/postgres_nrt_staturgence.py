"""
DAG pour la création des tables dans la bd unic_datamart pour le projet Near Real-Time (NRT) StatUrgence
"""
# pylint: disable=expression-not-assigned
from datetime import datetime

from airflow import DAG
from airflow.models import Param
from airflow.utils.trigger_rule import TriggerRule

from lib.config import default_args
from lib.groups.postgres.create_tables import create_tables
from lib.groups.postgres.drop_tables import drop_tables
from lib.slack import Slack
from lib.tasks.notify import end, start
from lib.tasks.postgres import PostgresResource, create_resource

DOC = """
# Postgres Near Real-Time (NRT) StatUrgence DAG

DAG pour la création des tables dans la bd unic_datamart pour le projet Near Real-Time (NRT) StatUrgence.

### Description
Ce DAG crée le schéma nrt_staturgence dans la base de données unic_datamart. Ensuite, les tables passées en
paramètre au DAG seront dropées et recréées selon les schémas définis dans `sql/nrt_staturgence`.

### Configuration
* Paramètre `tables` : Liste des tables à créer dans la base de données. Par défaut crée toutes les tables.
"""

# Specify path to SQL query for schema and each table
sql_config = {
    "schema": {"name": "nrt_staturgence", "postgres_schema_creation_sql_path": "sql/nrt_staturgence/schema.sql"},
    "tables": [
        {"name": "recherche_activity"            , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_activity.sql"            , "dependencies": []},
        {"name": "recherche_diagnostic"          , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_diagnostic.sql"          , "dependencies": []},
        {"name": "recherche_drug"                , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_drug.sql"                , "dependencies": []},
        {"name": "recherche_episode"             , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_episode.sql"             , "dependencies": []},
        {"name": "recherche_episode_activity"    , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_episode_activity.sql"    , "dependencies": []},
        {"name": "recherche_episode_bcm"         , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_episode_bcm.sql"         , "dependencies": []},
        {"name": "recherche_episode_consultation", "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_episode_consultation.sql", "dependencies": []},
        {"name": "recherche_episode_diagnostic"  , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_episode_diagnostic.sql"  , "dependencies": []},
        {"name": "recherche_episode_fadm"        , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_episode_fadm.sql"        , "dependencies": []},
        {"name": "recherche_episode_location"    , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_episode_location.sql"    , "dependencies": []},
        {"name": "recherche_episode_pec"         , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_episode_pec.sql"         , "dependencies": []},
        {"name": "recherche_episode_spec_action" , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_episode_spec_action.sql" , "dependencies": []},
        {"name": "recherche_episode_symptom"     , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_episode_symptom.sql"     , "dependencies": []},
        {"name": "recherche_episode_test"        , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_episode_test.sql"        , "dependencies": []},
        {"name": "recherche_episode_transfert"   , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_episode_transfert.sql"   , "dependencies": []},
        {"name": "recherche_episode_triage"      , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_episode_triage.sql"      , "dependencies": []},
        {"name": "recherche_generic"             , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_generic.sql"             , "dependencies": []},
        {"name": "recherche_location"            , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_location.sql"            , "dependencies": []},
        {"name": "recherche_locationsector"      , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_locationsector.sql"      , "dependencies": []},
        {"name": "recherche_logtransaction"      , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_logtransaction.sql"      , "dependencies": []},
        {"name": "recherche_patientadt"          , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_patientadt.sql"          , "dependencies": []},
        {"name": "recherche_patientadtlog"       , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_patientadtlog.sql"       , "dependencies": []},
        {"name": "recherche_patientallergy"      , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_patientallergy.sql"      , "dependencies": []},
        {"name": "recherche_pilotage"            , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_pilotage.sql"            , "dependencies": []},
        {"name": "recherche_quest_raison_visite" , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_quest_raison_visite.sql" , "dependencies": []},
        {"name": "recherche_test"                , "postgres_table_creation_sql_path": "sql/nrt_staturgence/tables/recherche_test.sql"                , "dependencies": []}
    ]
}
table_name_list = [table['name'] for table in sql_config['tables']]

# Update default args
args = default_args.copy()
args.update({
    'trigger_rule': TriggerRule.NONE_FAILED,
    'on_failure_callback': Slack.notify_task_failure})

with DAG(
        dag_id="postgres_nrt_staturgence",
        params={"tables": Param(table_name_list, type=['array'], examples=table_name_list)},
        default_args=args,
        doc_md=DOC,
        start_date=datetime(2024, 5, 7),
        is_paused_upon_creation=False,
        schedule_interval=None,
        max_active_tasks=1,
        tags=["postgresql"],
        on_failure_callback=Slack.notify_task_failure  # Should send notification to Slack when DAG exceeds timeout
) as dag:

    start("start_postgres_nrt_staturgence") >> create_resource(PostgresResource.SCHEMA, sql_config) \
        >> drop_tables(sql_config) >> create_tables(sql_config) >> end("publish_postgres_nrt_staturgence")
