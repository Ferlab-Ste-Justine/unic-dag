"""
Update Centro schema
"""
# pylint: disable=missing-function-docstring
from datetime import datetime, timedelta
from typing import List

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from core.config import default_args, default_timeout_hours, spark_failure_msg
from core.slack import Slack
from operators.spark import SparkOperator

JAR = 's3a://spark-prd/jars/unic-etl-master.jar'
NAMESPACE = 'raw'
MAIN_CLASS = 'bio.ferlab.ui.etl.schema.UpdateSchema'
DOC = """
# Update Centro schema DAG

ETL pour la mise à jour du schéma de Centro.

### Description
Cet ETL met à jour le schéma des tables de Centro en zone rouge, selon la spécification se trouvant dans le repo 
unic-etl. Le DAG lance une task par clinique.
"""

dag = DAG(
    dag_id="update_centro_schema",
    doc_md=DOC,
    start_date=datetime(2023, 8, 24),
    schedule_interval=None,
    dagrun_timeout=timedelta(hours=default_timeout_hours),
    default_args=default_args,
    is_paused_upon_creation=True,
    max_active_tasks=3,
    tags=["schema"]
)

with dag:
    def arguments(dataset_regex: str) -> List[str]:
        return ["config/prod.conf", "default", dataset_regex]


    start = EmptyOperator(
        task_id="start",
        on_execute_callback=Slack.notify_dag_start
    )

    aid = SparkOperator(
        task_id="update_centro_schema_aid",
        name="update-centro-schema-aid",
        arguments=arguments("raw_centro_aid_*"),
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=spark_failure_msg,
        spark_config="small-etl",
        dag=dag
    )

    chi = SparkOperator(
        task_id="update_centro_schema_chi",
        name="update-centro-schema-chi",
        arguments=arguments("raw_centro_chi_*"),
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=spark_failure_msg,
        spark_config="small-etl",
        dag=dag
    )

    chusj = SparkOperator(
        task_id="update_centro_schema_chusj",
        name="update-centro-schema-chusj",
        arguments=arguments("raw_centro_chusj_*"),
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=spark_failure_msg,
        spark_config="small-etl",
        dag=dag
    )

    cirene = SparkOperator(
        task_id="update_centro_schema_cirene",
        name="update-centro-schema-cirene",
        arguments=arguments("raw_centro_cirene_*"),
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=spark_failure_msg,
        spark_config="small-etl",
        dag=dag
    )

    ctc = SparkOperator(
        task_id="update_centro_schema_ctc",
        name="update-centro-schema-ctc",
        arguments=arguments("raw_centro_ctc_*"),
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=spark_failure_msg,
        spark_config="small-etl",
        dag=dag
    )

    dev = SparkOperator(
        task_id="update_centro_schema_dev",
        name="update-centro-schema-dev",
        arguments=arguments("raw_centro_dev_*"),
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=spark_failure_msg,
        spark_config="small-etl",
        dag=dag
    )

    fkp = SparkOperator(
        task_id="update_centro_schema_fkp",
        name="update-centro-schema-fkp",
        arguments=arguments("raw_centro_fkp_*"),
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=spark_failure_msg,
        spark_config="small-etl",
        dag=dag
    )

    gen = SparkOperator(
        task_id="update_centro_schema_gen",
        name="update-centro-schema-gen",
        arguments=arguments("raw_centro_gen_*"),
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=spark_failure_msg,
        spark_config="small-etl",
        dag=dag
    )

    inh = SparkOperator(
        task_id="update_centro_schema_inh",
        name="update-centro-schema-inh",
        arguments=arguments("raw_centro_inh_*"),
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=spark_failure_msg,
        spark_config="small-etl",
        dag=dag
    )

    neo = SparkOperator(
        task_id="update_centro_schema_neo",
        name="update-centro-schema-neo",
        arguments=arguments("raw_centro_neo_*"),
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=spark_failure_msg,
        spark_config="small-etl",
        dag=dag
    )

    neu = SparkOperator(
        task_id="update_centro_schema_neu",
        name="update-centro-schema-neu",
        arguments=arguments("raw_centro_neu_*"),
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=spark_failure_msg,
        spark_config="small-etl",
        dag=dag
    )

    obg = SparkOperator(
        task_id="update_centro_schema_obg",
        name="update-centro-schema-obg",
        arguments=arguments("raw_centro_obg_*"),
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=spark_failure_msg,
        spark_config="small-etl",
        dag=dag
    )

    obs = SparkOperator(
        task_id="update_centro_schema_obs",
        name="update-centro-schema-obs",
        arguments=arguments("raw_centro_obs_*"),
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=spark_failure_msg,
        spark_config="small-etl",
        dag=dag
    )

    ped = SparkOperator(
        task_id="update_centro_schema_ped",
        name="update-centro-schema-ped",
        arguments=arguments("raw_centro_ped_*"),
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=spark_failure_msg,
        spark_config="small-etl",
        dag=dag
    )

    psy = SparkOperator(
        task_id="update_centro_schema_psy",
        name="update-centro-schema-psy",
        arguments=arguments("raw_centro_psy_*"),
        namespace=NAMESPACE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=spark_failure_msg,
        spark_config="small-etl",
        dag=dag
    )

    pul = SparkOperator(
        task_id="update_centro_schema_pul",
        name="update-centro-schema-pul",
        arguments=arguments("raw_centro_pul_*"),
        namespace=NAMESPACE,
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

    start >> [aid, chi, chusj, cirene, ctc, dev, fkp, gen, inh, neo, neu, obg, obs, ped, psy, pul] >> end
