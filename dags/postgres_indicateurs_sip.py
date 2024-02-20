"""
DAG pour la création des table dans la bd unic_datamart pour indicteursSip
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from core.slack import Slack

DOC = """
# Creation de tables dans le bd postgreSQL 

ETL pour la creation de tables dans unic_datamart pour indicateursSip
"""

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

    create_schema = PostgresOperator(
        task_id="create_schema",
        postgres_conn_id="postgresql-bi-rw",
        sql="sql/indicateurs_sip/schema.sql"
    )

    create_sejour_table = PostgresOperator(
        task_id="create_sejour_table",
        postgres_conn_id="postgresql-bi-rw",
        sql="sql/indicateurs_sip/tables/sejour_schema.sql"
    )

    create_catheter_table = PostgresOperator(
        task_id="create_catheter_table",
        postgres_conn_id="postgresql-bi-rw",
        sql="sql/indicateurs_sip/tables/catheter_schema.sql"
    )

    create_extubation_table = PostgresOperator(
        task_id="create_extubation_table",
        postgres_conn_id="postgresql-bi-rw",
        sql="sql/indicateurs_sip/tables/extubation_schema.sql"
    )

    create_ventilation_table = PostgresOperator(
        task_id="create_ventilation_table",
        postgres_conn_id="postgresql-bi-rw",
        sql="sql/indicateurs_sip/tables/ventilation_schema.sql"
    )

    end = EmptyOperator(
        task_id="publish_postgres_indicateurs_sip",
        on_success_callback=Slack.notify_dag_completion
    )

    start >> create_schema >> [create_sejour_table,
                               create_catheter_table,
                               create_extubation_table,
                               create_ventilation_table] >> end
