"""
DAG that handles the ETL process for curated Philips.
"""

from datetime import timedelta


from airflow import DAG
from airflow.operators.empty import EmptyOperator

from core.config import default_args, spark_failure_msg, jar, default_params
from operators.spark import SparkOperator


ZONE = 'red'
TAGS = ['curated']
DAG_ID = 'curated_quanum_chartmaxx'
MAIN_CLASS = 'bio.ferlab.ui.etl.red.curated.quanumchartmaxx.Main'
DOC = 'DAG that handles the ETL process for curated quanum_chartmaxx.'


dag_args = default_args.copy()
dag_args.update({
    'provide_context': True,
})


dag = DAG(
    dag_id=DAG_ID,
    doc_md=DOC,
    schedule_interval=None,
    params=default_params,
    dagrun_timeout=timedelta(hours=4),
    default_args=dag_args,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=TAGS
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
        "--config", "config/prod.conf",
        "--steps", "initial",
        "--app-name", destination,
        "--destination", destination
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

    start = EmptyOperator(
        task_id='start_curated_quanum_chartmaxx',
    )

    spark_task_configs = [
        ('curated_quanum_chartmaxx_a*', 'large'),
        ('curated_quanum_chartmaxx_b*', 'medium'),
        ('curated_quanum_chartmaxx_c*', 'large'),
        ('curated_quanum_chartmaxx_dossier*', 'medium'),
        ('curated_quanum_chartmaxx_e*', 'medium'),
        ('curated_quanum_chartmaxx_f*', 'medium'),
        ('curated_quanum_chartmaxx_g*', 'medium'),
        ('curated_quanum_chartmaxx_i*', 'medium'),
        ('curated_quanum_chartmaxx_l*', 'medium'),
        ('curated_quanum_chartmaxx_a*', 'large'),
        ('curated_quanum_chartmaxx_maladie*', 'large'),
        ('curated_quanum_chartmaxx_n*', 'medium'),
        ('curated_quanum_chartmaxx_o*', 'medium'),
        ('curated_quanum_chartmaxx_p*', 'large'),
        ('curated_quanum_chartmaxx_q*', 'medium'),
        ('curated_quanum_chartmaxx_r*', 'large'),
        ('curated_quanum_chartmaxx_s*', 'medium'),
        ('curated_quanum_chartmaxx_t*', 'large'),
        ('curated_quanum_chartmaxx_urogynecologie*', 'large'),
        ('curated_quanum_chartmaxx_v*', 'medium'),
    ]

    spark_tasks = [create_spark_task(destination, cluster_size) for destination, cluster_size in spark_task_configs]

    end = EmptyOperator(
        task_id='publish_curated_quanum_chartmaxx',
    )

    start >> spark_tasks >> end
