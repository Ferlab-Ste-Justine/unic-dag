"""
DAG pour la publication de tables dans la bd unic_datamart pour indicteursSip
"""
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

from core.slack import Slack
from operators.copy_csv_to_postgres import CopyCsvToPostgres

DOC = """
# Released to Published Indicateurs SIP 

ETL pour copier les csv released de indicateurs sip dans la bd PostgreSQL de Power BI.
"""

CA_PATH = '/tmp/ca/bi/'  # must correspond to path in postgres connection string
CA_FILENAME = 'ca.crt'  # must correspond to filename in postgres connection string
CA_CERT = Variable.get('postgres_ca_certificate', None)
MINIO_CONN = Variable.get('conn_minio', None)

with DAG(
        dag_id="published_indicateurs_sip",
        doc_md=DOC,
        start_date=datetime(2024, 2, 26),
        is_paused_upon_creation=True,
        schedule_interval=None,
        max_active_tasks=1,
        tags=["published"]
) as dag:

    copy_conf = [
        {"src_s3_bucket" :  "green-prd", "src_s3_key" :  "released/indicateurssip/catheter/catheter.csv"      , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "catheter"   },
        {"src_s3_bucket" :  "green-prd", "src_s3_key" :  "released/indicateurssip/extubation/extubation.csv"  , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "extubation" },
        {"src_s3_bucket" :  "green-prd", "src_s3_key" :  "released/indicateurssip/sejour/sejour.csv"          , "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "sejour"     },
        {"src_s3_bucket" :  "green-prd", "src_s3_key" :  "released/indicateurssip/ventilation/ventilation.csv", "dts_postgres_schema" : "indicateurs_sip", "dts_postgres_tablename" : "ventilation"}
    ]

    start = EmptyOperator(
        task_id="start_published_indicateurs_sip",
        on_execute_callback=Slack.notify_dag_start
    )

    copy_csv = CopyCsvToPostgres(
        task_id="published_indicateurs_sip",
        postgres_conn_id="postgresql_bi_rw",
        ca_path=CA_PATH,
        ca_filename=CA_FILENAME,
        ca_cert=CA_CERT,
        table_copy_conf=copy_conf,
        minio_conn_id=MINIO_CONN
    )

    end = EmptyOperator(
        task_id="publish_published_indicateurs_sip",
        on_success_callback=Slack.notify_dag_completion
    )

    start >> copy_csv >> end
