"""
Update Centro schema
"""
# pylint: disable=missing-function-docstring, expression-not-assigned
from datetime import datetime, timedelta
from typing import List

from airflow import DAG

from lib.config import default_args, default_timeout_hours, spark_failure_msg
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.notify import end, start

JAR = 's3a://spark-prd/jars/unic-etl-master.jar'
ZONE = 'red'
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
    tags=["schema"],
    on_failure_callback=Slack.notify_dag_failure  # Should send notification to Slack when DAG exceeds timeout
)

with dag:
    def arguments(dataset_regex: str) -> List[str]:
        return ["config/prod.conf", "default", dataset_regex]

    aid = SparkOperator(
        task_id="update_centro_schema_aid",
        name="update-centro-schema-aid",
        arguments=arguments("raw_centro_aid_*"),
        zone=ZONE,
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
        zone=ZONE,
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
        zone=ZONE,
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
        zone=ZONE,
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
        zone=ZONE,
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
        zone=ZONE,
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
        zone=ZONE,
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
        zone=ZONE,
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
        zone=ZONE,
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
        zone=ZONE,
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
        zone=ZONE,
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
        zone=ZONE,
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
        zone=ZONE,
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
        zone=ZONE,
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
        zone=ZONE,
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
        zone=ZONE,
        spark_class=MAIN_CLASS,
        spark_jar=JAR,
        spark_failure_msg=spark_failure_msg,
        spark_config="small-etl",
        dag=dag
    )

    start() >> [aid, chi, chusj, cirene, ctc, dev, fkp, gen, inh, neo, neu, obg, obs, ped, psy, pul] >> end()
