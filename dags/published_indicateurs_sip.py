"""
DAG pour la publication de tables dans la bd unic_datamart pour indicteursSip
"""
import json
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

from core.slack import Slack
from dags.operators.copy_csv_to_postgres import CopyCsvToPostgres

DOC = """
# Released to Published Indicateurs SIP 

ETL pour copier les csv released de indicateurs sip dans la bd PostgreSQL de Power BI.
"""

CA_PATH = '/tmp/ca/bi/'  # must correspond to path in postgres connection string
CA_FILENAME = 'ca.crt'  # must correspond to filename in postgres connection string
CA_CERT = Variable.get('postgres_ca_certificate', None)
MINIO_CONN = Variable.get('conn_minio', None)
config = json.load(open('dags/config/green/indicateurs_sip_onfig.json', encoding='UTF8'))

with DAG(
        dag_id="published_indicateurs_sip",
        doc_md=DOC,
        start_date=datetime(2024, 2, 26),
        is_paused_upon_creation=True,
        schedule_interval=None,
        max_active_tasks=1,
        tags=["published"]
) as dag:

    start = EmptyOperator(
        task_id="start_published_indicateurs_sip",
        on_execute_callback=Slack.notify_dag_start
    )

    copy_csv = CopyCsvToPostgres(
        task_id=f"published_indicateurs_sip",
        postgres_conn_id="postgresql_bi_rw",
        ca_path=CA_PATH,
        ca_filename=CA_FILENAME,
        ca_cert=CA_CERT,
        table_copy_conf=[table['copy_conf'] for table in config['tables']],
        minio_conn_id=MINIO_CONN,
        schema=config['schema']['name']
    )

    end = EmptyOperator(
        task_id="publish_published_indicateurs_sip",
        on_success_callback=Slack.notify_dag_completion
    )

    start >> copy_csv >> end
