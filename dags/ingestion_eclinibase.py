from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

from yml.spark_operator_yml import read_json, create_spark_job, check_spark_job

# DEFAULT_ARGS = generate_default_args(owner="cbotek", on_failure_callback=task_fail_slack_alert)
DEFAULT_ARGS = {
    "owner": "cbotek",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "cbotek@ferlab.bio"
}
NAMESPACE = "ingestion"
CONFIG_FILE = "config/prod.conf"
SCHEMA = "eclinibase"
dagid = f"{NAMESPACE}_{SCHEMA}_static".lower()
args = DEFAULT_ARGS

config_file = CONFIG_FILE

with DAG(
        dag_id=dagid,
        schedule_interval=None,
        default_args=args,
        start_date=days_ago(2),
        concurrency=2,
        catchup=False
) as dag:
    start = DummyOperator(
        task_id="start_operator",
        dag=dag
    )

    config = read_json(f"/opt/airflow/dags/repo/dags/config/ingestion/{SCHEMA}_config.json")

    for conf in config:
        spark_operator_conf = {
            "job_type": NAMESPACE,
            "schema": SCHEMA,
            "namespace": NAMESPACE,
            "destination": conf['dataset_id'],
            "etl_config_file": "config/prod.conf",
            "run_type": conf['run_type']
        }
        trigger_job = TriggerDagRunOperator(task_id=f"trigger_{conf['dataset_id']}",
                                            trigger_dag_id="spark_operator_job",
                                            conf={"destination": conf['dataset_id']},
                                            wait_for_completion=True,
                                            poke_interval=30)

        # create_job = create_spark_job(conf['dataset_id'], NAMESPACE, conf['run_type'], config_file, dag)
        # check_job = check_spark_job(conf['dataset_id'], NAMESPACE, dag)

        start >> trigger_job
