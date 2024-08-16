"""
DAG pour la création des tables dans la bd unic_db pour le catalogue de l'UnIC.
"""
# pylint: disable=missing-function-docstring, expression-not-assigned

from datetime import datetime

from airflow import DAG
from airflow.models import Param
from airflow.utils.trigger_rule import TriggerRule

from lib.groups.postgres.create_tables import create_tables
from lib.postgres import postgres_vlan2_ca_path, postgres_vlan2_ca_cert, \
    unic_dev_postgres_vlan2_conn_id
from lib.slack import Slack
from lib.tasks.notify import start, end
from lib.tasks.postgres import create_resource, PostgresResource

DOC = """
# Postgres Catalog DAG

DAG pour la création des tables dans la bd `unic_db` pour le catalogue de l'UnIC.

### Description
Ce DAG crée le schéma `catalog` et ses types dans la base de données `unic_db`. Ensuite, les tables passées en
paramètre au DAG seront dropées et recréées selon les schémas définis dans `sql/catalog`. Finalement, des indexes sont
créés sur les tables.

### Configuration
* Paramètre `tables` : Liste des tables à créer dans la base de données. Par défaut, crée toutes les tables.
* Paramètre `env` : Environnement de la BD. Par défaut, 'prod'.
"""

# Specify path to SQL query for schema and each table
sql_config = {
    "schema": {"name": "catalog", "postgres_schema_creation_sql_path": "sql/catalog/schema.sql"},
    "types": {"name": "catalog", "postgres_types_creation_sql_path": "sql/catalog/types.sql"},
    "tables": [
        {"name": "resource"      , "postgres_table_creation_sql_path": "sql/catalog/tables/resource.sql"      , "dependencies": ["analyst"]},
        {"name": "analyst"       , "postgres_table_creation_sql_path": "sql/catalog/tables/analyst.sql"       , "dependencies": []},
        {"name": "value_set"     , "postgres_table_creation_sql_path": "sql/catalog/tables/value_set.sql"     , "dependencies": []},
        {"name": "dict_table"    , "postgres_table_creation_sql_path": "sql/catalog/tables/dict_table.sql"    , "dependencies": ["resource"]},
        {"name": "variable"      , "postgres_table_creation_sql_path": "sql/catalog/tables/variable.sql"      , "dependencies": ["dict_table", "value_set"]},
        {"name": "value_set_code", "postgres_table_creation_sql_path": "sql/catalog/tables/value_set_code.sql", "dependencies": ["value_set"]},
        {"name": "mapping"       , "postgres_table_creation_sql_path": "sql/catalog/tables/mapping.sql"       , "dependencies": ["value_set_code"]},
        {"name": "user"          , "postgres_table_creation_sql_path": "sql/catalog/tables/user.sql"          , "dependencies": []},
        {"name": "refresh_token" , "postgres_table_creation_sql_path": "sql/catalog/tables/refresh_token.sql" , "dependencies": ["user"]}
    ],
    "indexes": {"name": "catalog", "postgres_indexes_creation_sql_path": "sql/catalog/indexes.sql"}
}
table_name_list = [table['name'] for table in sql_config['tables']]

with DAG(
        dag_id="postgres_catalog",
        params={
            "tables": Param(table_name_list, type=['array'], examples=table_name_list),
            "env": Param("dev", type="string", enum=["dev", "prod"])
        },
        default_args={
            'trigger_rule': TriggerRule.NONE_FAILED,
            'on_failure_callback': Slack.notify_task_failure,
        },
        doc_md=DOC,
        start_date=datetime(2024, 5, 13),
        is_paused_upon_creation=False,
        schedule_interval=None,
        max_active_tasks=1,
        tags=["postgresql"]
) as dag:
    def get_conn_id() -> str:
        # dev conn id is temporarily hardcoded until we have a propre dev Airflow instance
        return unic_dev_postgres_vlan2_conn_id

    start("start_postgres_catalog") \
        >> create_resource(PostgresResource.SCHEMA, sql_config, conn_id=get_conn_id(), ca_path=postgres_vlan2_ca_path, ca_cert=postgres_vlan2_ca_cert) \
        >> create_resource(PostgresResource.TYPES, sql_config, conn_id=get_conn_id(), ca_path=postgres_vlan2_ca_path, ca_cert=postgres_vlan2_ca_cert) \
        >> create_tables(sql_config, conn_id=get_conn_id(), ca_path=postgres_vlan2_ca_path, ca_cert=postgres_vlan2_ca_cert) \
        >> create_resource(PostgresResource.INDEXES, sql_config, conn_id=get_conn_id(), ca_path=postgres_vlan2_ca_path, ca_cert=postgres_vlan2_ca_cert) \
        >> end("end_postgres_catalog")
