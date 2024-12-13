"""
DAG that handles the ETL process for curated Philips.
"""
# pylint: disable=expression-not-assigned

from datetime import datetime, timedelta

import pendulum
from airflow import DAG

from lib.tasks.optimize import optimize
from lib.config import default_args, spark_failure_msg, jar, default_params
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.notify import start, end

ZONE = 'red'
TAGS = ['curated']
DAG_ID = 'curated_philips'
MAIN_CLASS = 'bio.ferlab.ui.etl.red.curated.philips.Main'
DOC = 'DAG that handles the ETL process for curated Philips data.'
CONFIG = "config/prod.conf"


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
    schedule_interval=None,
    params=default_params,
    dagrun_timeout=timedelta(hours=8),
    default_args=dag_args,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=TAGS,
    on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
)

def create_spark_task(destination, cluster_size):
    """
    Create a SparkOperator task for the ETL process

    Args:
        destination (str): name of the destination data to be processed
        cluster_size (str): size of cluster used

    Returns:
        SparkOperator
    """

    args = [
        "--config", CONFIG,
        "--steps", "initial",
        "--app-name", destination,
        "--destination", destination,
        "--date", "{{ds}}"
    ]

    return SparkOperator(
        task_id=destination,
        name=destination,
        arguments=args,
        zone=ZONE,
        spark_class=MAIN_CLASS,
        spark_jar=jar,
        spark_failure_msg=spark_failure_msg,
        spark_config=cluster_size,
        dag=dag
    )

with dag:

    spark_task_configs = [
        ('curated_philips_sip_external_patient', 'medium-etl'),
        ('curated_philips_neo_external_patient', 'medium-etl'),
    ]

    spark_tasks = [create_spark_task(destination, cluster_size) for destination, cluster_size in spark_task_configs]

    optimization_task = optimize(['curated_philips_sip_external_patient', 'curated_philips_neo_external_patient']
                                  , "phillips", ZONE, "curated", CONFIG, jar, dag)

    start('start_curated_philips') >> spark_tasks >> optimization_task >> end('end_curated_philips')
