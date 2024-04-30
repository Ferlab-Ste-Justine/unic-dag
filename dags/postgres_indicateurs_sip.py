"""
DAG pour la création des table dans la bd unic_datamart pour indicteursSip
"""
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

from core.slack import Slack
from operators.postgresca import PostgresCaOperator

DOC = """
# Creation de tables dans le bd postgreSQL 

ETL pour la creation de tables dans unic_datamart pour indicateursSip
"""

CA_PATH = '/tmp/ca/bi/'  # must correspond to path in postgres connection string
CA_FILENAME = 'ca.crt'  # must correspond to filename in postgres connection string
CA_CERT = Variable.get('postgres_ca_certificate', None)

with DAG(
        dag_id="postgres_indicateurs_sip",
        doc_md=DOC,
        start_date=datetime(2024, 2, 16),
        is_paused_upon_creation=True,
        schedule_interval=None,
        max_active_tasks=1,
        tags=["postgresql"]
) as dag:

    # Specify path to SQL query for schema and each table
    sql_config = {
        "schema": {"name" : "indicateurs_sip", "postgres_schema_creation_sql_path" : "sql/indicateurs_sip/schema.sql"},
        "tables": [
            # {"name": "catheter"   , "postgres_table_creation_sql_path" : "sql/indicateurs_sip/tables/catheter_schema.sql"   },
            # {"name": "extubation" , "postgres_table_creation_sql_path" : "sql/indicateurs_sip/tables/extubation_schema.sql" },
            # {"name": "sejour"     , "postgres_table_creation_sql_path" : "sql/indicateurs_sip/tables/sejour_schema.sql"     },
            # {"name": "ventilation", "postgres_table_creation_sql_path" : "sql/indicateurs_sip/tables/ventilation_schema.sql"},
            # {"name": "lits"       , "postgres_table_creation_sql_path" : "sql/indicateurs_sip/tables/lits_schema.sql"},
            {"name": "infirmieres" , "postgres_table_creation_sql_path" : "sql/indicateurs_sip/tables/infirmieres_schema.sql"},
        ]
    }

    start = EmptyOperator(
        task_id="start_postgres_indicateurs_sip",
        on_execute_callback=Slack.notify_dag_start
    )

    #WARNING THIS WILL DROP TABLES
    drop_tables = PostgresCaOperator(
        task_id="drop_tables",
        postgres_conn_id="postgresql_bi_rw",
        sql="sql/indicateurs_sip/tables/drop_tables.sql",
        ca_path=CA_PATH,
        ca_filename=CA_FILENAME,
        ca_cert=CA_CERT,
    )

    create_table_tasks = [PostgresCaOperator(
        task_id=f"create_{table_config['name']}_table",
        postgres_conn_id="postgresql_bi_rw",
        sql= table_config['postgres_table_creation_sql_path'],
        ca_path=CA_PATH,
        ca_filename=CA_FILENAME,
        ca_cert=CA_CERT,
    ) for table_config in sql_config['tables']]

    end = EmptyOperator(
        task_id="publish_postgres_indicateurs_sip",
        on_success_callback=Slack.notify_dag_completion
    )

    start >> drop_tables >> create_table_tasks >> end
