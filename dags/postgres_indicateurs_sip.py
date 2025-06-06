"""
DAG pour la création des tables dans la bd unic_datamart pour le projet Indicateurs SIP
"""
# pylint: disable=expression-not-assigned

from datetime import datetime

from airflow import DAG
from airflow.models import Param
from airflow.utils.trigger_rule import TriggerRule

from lib.config import DEFAULT_ARGS
from lib.groups.postgres.create_tables import create_tables
from lib.groups.postgres.drop_tables import drop_tables
from lib.slack import Slack
from lib.tasks.notify import start, end
from lib.tasks.postgres import PostgresResource, create_resource

DOC = """
# Postgres Indicateurs SIP DAG

DAG pour la création des tables dans la bd unic_datamart pour le projet Indicateurs SIP.

### Description
Ce DAG crée le schéma indicateurs_sip dans la base de données unic_datamart. Ensuite, les tables passées en
paramètre au DAG seront dropées et recréées selon les schémas définis dans `sql/indicateurs_sip`.

### Configuration
* Paramètre `tables` : Liste des tables à créer dans la base de données. Par défaut crée toutes les tables.
"""

# Specify path to SQL query for schema and each table
sql_config = {
    "schema": {"name" : "indicateurs_sip", "postgres_schema_creation_sql_path" : "sql/indicateurs_sip/schema.sql"},
    "tables": [
        {"name": "sejour"     , "postgres_table_creation_sql_path": "sql/indicateurs_sip/tables/sejour_schema.sql"     , "dependencies": []},
        {"name": "catheter"   , "postgres_table_creation_sql_path": "sql/indicateurs_sip/tables/catheter_schema.sql"   , "dependencies": []},
        {"name": "ventilation", "postgres_table_creation_sql_path": "sql/indicateurs_sip/tables/ventilation_schema.sql", "dependencies": []},
        {"name": "extubation" , "postgres_table_creation_sql_path": "sql/indicateurs_sip/tables/extubation_schema.sql" , "dependencies": []},
        {"name": "lits"       , "postgres_table_creation_sql_path": "sql/indicateurs_sip/tables/lits_schema.sql"       , "dependencies": []},
        {"name": "infirmieres", "postgres_table_creation_sql_path": "sql/indicateurs_sip/tables/infirmieres_schema.sql", "dependencies": []},
        {"name": "ecmo"       , "postgres_table_creation_sql_path": "sql/indicateurs_sip/tables/ecmo_schema.sql"       , "dependencies": []},
        {"name": "scores"     , "postgres_table_creation_sql_path": "sql/indicateurs_sip/tables/scores_schema.sql"     , "dependencies": []},
        {"name": "infections" , "postgres_table_creation_sql_path": "sql/indicateurs_sip/tables/infections_schema.sql" , "dependencies": []},
    ]
}
table_name_list = [table['name'] for table in sql_config['tables']]

# Update default args
args = DEFAULT_ARGS.copy()
args.update({
    'trigger_rule': TriggerRule.NONE_FAILED,
    'on_failure_callback': Slack.notify_task_failure})

with DAG(
        dag_id="postgres_indicateurs_sip",
        params={"tables": Param(table_name_list, type=['array'], examples=table_name_list)},
        default_args=args,
        doc_md=DOC,
        start_date=datetime(2024, 2, 16),
        is_paused_upon_creation=False,
        schedule_interval=None,
        max_active_tasks=1,
        tags=["postgresql"],
        on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
) as dag:

    start("start_postgres_indicateurs_sip") >> create_resource(PostgresResource.SCHEMA, sql_config) \
        >> drop_tables(sql_config) >> create_tables(sql_config) >> end("publish_postgres_indicateurs_sip")
