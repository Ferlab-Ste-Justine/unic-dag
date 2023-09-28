"""
DAG that handles the ETL process for curated Philips.
"""

from datetime import datetime, timedelta
import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from core.config import default_args, spark_failure_msg, jar, default_params
from core.slack import Slack
from operators.spark import SparkOperator


NS = 'curated'
TAGS = ['curated']
DAG_ID = 'curated_philips'
MAIN_CLASS = 'bio.ferlab.ui.etl.red.curated.philips.Main'
DOC = 'DAG that handles the ETL process for curated Philips data.'


dag_args = default_args.copy()
dag_args.update({
    'start_date': datetime(2023, 9, 27, tzinfo=pendulum.timezone("America/Montreal")), # put this date only to test
    'provide_context': True,
    'depends_on_past': True,
    'wait_for_downstream': True
})

dag = DAG(
    dag_id=DAG_ID,
    doc_md=DOC,
    start_date=datetime(2023, 9, 27, tzinfo=pendulum.timezone("America/Montreal")), # put this date only to test
    schedule_interval='0 0 * * *',
    params=default_params,
    dagrun_timeout=timedelta(hours=2),
    default_args=dag_args,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=TAGS
)

with dag:

    start = EmptyOperator(
        task_id='start_curated_philips',
        on_execute_callback=Slack.notify_dag_start
    )

    curated_philips_sip_external_patient = SparkOperator(
        task_id='curated_philips_sip_external_patient',
        name='curated_philips_sip_external_patient',
        arguments=['config/prod.conf', 'initial', 'curated_philips_sip_external_patient', '{{ds}}'],
        namespace=NS,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config='medium-etl',
        dag=dag
    )

    curated_philips_neo_external_patient = SparkOperator(
        task_id='curated_philips_neo_external_patient',
        name='curated_philips_neo_external_patient',
        arguments=['config/prod.conf', 'initial', 'curated_philips_neo_external_patient', '{{ds}}'],
        namespace=NS,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config='medium-etl',
        dag=dag
    )

    end = EmptyOperator(
        task_id='publish_curated_philips',
        on_success_callback=Slack.notify_dag_completion
    )

    start >> [curated_philips_sip_external_patient, curated_philips_neo_external_patient] >> end
