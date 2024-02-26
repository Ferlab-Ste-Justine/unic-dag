"""
DAG pour la création des table dans la bd unic_datamart pour indicteursSip
"""
import json
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
config = json.load(open('dags/config/green/indicateurs_sip_onfig.json', encoding='UTF8'))

with DAG(
        dag_id="postgres_indicateurs_sip",
        doc_md=DOC,
        start_date=datetime(2024, 2, 16),
        is_paused_upon_creation=True,
        schedule_interval=None,
        max_active_tasks=1,
        tags=["postgresql"]
) as dag:

    start = EmptyOperator(
        task_id="start_postgres_indicateurs_sip",
        on_execute_callback=Slack.notify_dag_start
    )

    create_schema = PostgresCaOperator(
        task_id="create_schema",
        postgres_conn_id="postgresql_bi_rw",
        sql=config['schema']['postgres_schema_creation_sql_path'],
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
    ) for table_config in config['tables']]

    end = EmptyOperator(
        task_id="publish_postgres_indicateurs_sip",
        on_success_callback=Slack.notify_dag_completion
    )

    start >> create_schema >> create_table_tasks >> end
