"""
Update Softlab Partition
"""
# pylint: disable=missing-function-docstring
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from core.config import default_args, default_timeout_hours, spark_failure_msg
from core.slack import Slack
from operators.spark import SparkOperator

JAR = 's3a://spark-prd/jars/unic-etl-{{ params.branch }}.jar'
ZONE = 'red'
MAIN_CLASS = 'bio.ferlab.ui.etl.schema.UpdateSchema'
DOC = """
# Update Softlab Partition DAG

ETL pour la mise à jour des partitions de Softlab.

### Description
Cet ETL met à jour les partitions de la table anonymized_softlab_v_p_lab_message en zone rouge, selon la spécification se trouvant dans le repo 
unic-etl.
"""

dag = DAG(
    dag_id="update_softlab_partition",
    doc_md=DOC,
    start_date=datetime(2023, 10, 18),
    schedule_interval=None,
    dagrun_timeout=timedelta(hours=default_timeout_hours),
    default_args=default_args,
    is_paused_upon_creation=True,
    max_active_tasks=3,
    tags=["schema"]
)

with dag:

    start = EmptyOperator(
        task_id="start",
        on_execute_callback=Slack.notify_dag_start
    )

    v_p_lab_message = SparkOperator(
        task_id="update_v_p_lab_message_partition",
        name="update-v-p-lab-message-partition",
        arguments=["config/prod.conf", "default", "raw_softlab_v_p_lab_message"],
        zone=ZONE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=spark_failure_msg,
        spark_config="small-etl",
        dag=dag
    )

    end = EmptyOperator(
        task_id="end",
        on_success_callback=Slack.notify_dag_completion
    )

    start >> v_p_lab_message >> end
