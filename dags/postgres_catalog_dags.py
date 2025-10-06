"""
Génération des DAGSs pour la création des tables dans la bd unic_db pour le catalogue de l'UnIC.
Un DAG par environnement postgres est généré.
"""
# pylint: disable=missing-function-docstring, expression-not-assigned, duplicate-code

from datetime import datetime

from airflow import DAG
from airflow.models import Param
from airflow.utils.trigger_rule import TriggerRule

from lib.config import DEFAULT_ARGS
from lib.groups.postgres.create_tables import create_tables
from lib.postgres import POSTGRES_VLAN2_CA_PATH, \
    PostgresEnv, unic_postgres_vlan2_conn_id, unic_postgres_vlan2_ca_cert
from lib.slack import Slack
from lib.tasks.notify import start, end
from lib.tasks.postgres import create_resource, PostgresResource

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

# Update default args
args = DEFAULT_ARGS.copy()
args.update({
    'trigger_rule': TriggerRule.NONE_FAILED,
    'on_failure_callback': Slack.notify_task_failure})

for env in PostgresEnv:
    env_name = env.value
    conn_id = unic_postgres_vlan2_conn_id(env)
    ca_cert = unic_postgres_vlan2_ca_cert(env)

    doc = f"""
    # Postgres **{env_name}** Catalog DAG
    
    DAG pour la création des tables dans la bd `unic_db` pour le catalogue de l'UnIC dans l'environnement **{env_name}**.
    
    ### Description
    Ce DAG crée le schéma `catalog` et ses types dans la base de données `unic_db`. Ensuite, les tables passées en
    paramètre au DAG seront dropées et recréées selon les schémas définis dans `sql/catalog`. Finalement, des indexes sont
    créés sur les tables.
    
    ### Configuration
    * Paramètre `tables` : Liste des tables à créer dans la base de données. Par défaut, crée toutes les tables.
    """

    with DAG(
            dag_id=f"postgres_{env_name}_catalog",
            params={
                "tables": Param(table_name_list, type=['array'], examples=table_name_list),
            },
            default_args=args,
            doc_md=doc,
            start_date=datetime(2024, 5, 13),
            is_paused_upon_creation=False,
            schedule_interval=None,
            max_active_tasks=1,
            tags=["postgresql"],
            on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
    ) as dag:
        start("start_postgres_catalog") \
        >> create_resource(PostgresResource.SCHEMA, sql_config, conn_id=conn_id, ca_path=POSTGRES_VLAN2_CA_PATH,
                           ca_cert=ca_cert) \
        >> create_resource(PostgresResource.TYPES, sql_config, conn_id=conn_id, ca_path=POSTGRES_VLAN2_CA_PATH,
                           ca_cert=ca_cert) \
        >> create_tables(sql_config, conn_id=conn_id, ca_path=POSTGRES_VLAN2_CA_PATH, ca_cert=ca_cert) \
        >> create_resource(PostgresResource.INDEXES, sql_config, conn_id=conn_id, ca_path=POSTGRES_VLAN2_CA_PATH,
                           ca_cert=ca_cert) \
        >> end("end_postgres_catalog")
