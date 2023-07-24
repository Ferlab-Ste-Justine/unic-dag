"""
EmailOperator POC DAG
"""
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from sqlalchemy_utils.types.enriched_datetime.pendulum_date import pendulum

dag = DAG(
    dag_id="poc_email",
    start_date=datetime(2023, 7, 14, 0, tzinfo=pendulum.timezone("America/Montreal")),
    schedule_interval=None,
    is_paused_upon_creation=True,
    tags=["poc"]
)

with dag:
    email_destination = Variable.get("EMAIL_ENRICHED_SIGNATURE_MAIL_TO")

    EmailOperator(
        task_id="send_email",
        to=email_destination,
        subject="airflow test",
        html_content="Automated email sent from Airflow on {{ ds }}."
    )
