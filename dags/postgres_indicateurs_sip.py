"""
DAG pour la création des table dans la bd unic_datamart pour indicteursSip
"""
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
        is_paused_upon_creation=True,
        max_active_tasks=1,
        tags=["postgresql"]
) as dag:
    start_postgres_indicateurs_sip = EmptyOperator(
        task_id="start_postgres_indicateurs_sip",
        on_execute_callback=Slack.notify_dag_start
    )

    create_sejour_table = PostgresOperator(
        task_id="create_sejour_table",
        postgres_conn_id="unic-prod-postgresql-bi-rw-conn-airflow",
        sql="sql/sejour_schema.sql"
    )

    create_catheter_table = PostgresOperator(
        task_id="create_catheter_table",
        postgres_conn_id="unic-prod-postgresql-bi-rw-conn-airflow",
        sql="sql/catheter_schema.sql"
    )

    create_extubation_table = PostgresOperator(
        task_id="create_extubation_table",
        postgres_conn_id="unic-prod-postgresql-bi-rw-conn-airflow",
        sql="sql/extubation_schema.sql"
    )

    create_ventilation_table = PostgresOperator(
        task_id="create_ventilation_table",
        postgres_conn_id="postgresql",
        sql="sql/ventilation_schema.sql"
    )

    publish_postgres_indicateurs_sip = EmptyOperator(
        task_id="publish_postgres_indicateurs_sip",
        on_success_callback=Slack.notify_dag_completion
    )

    start_postgres_indicateurs_sip >> [create_sejour_table,
                                       create_catheter_table,
                                       create_extubation_table,
                                       create_ventilation_table] >> publish_postgres_indicateurs_sip
