from datetime import timedelta, datetime

import pendulum
from airflow.models import Variable, Param

from lib.failure import Failure
from lib.slack import Slack

# DAGs config
ROOT = Variable.get('dags_path', '/opt/airflow/dags/repo/dags')
DAGS_CONFIG_PATH = f"{ROOT}/config"
EXTRACT_RESOURCE = '(.*)_config.json'
DEFAULT_START_DATE = datetime(2021, 1, 1, tzinfo=pendulum.timezone("America/Montreal"))
DEFAULT_TIMEOUT_HOURS = 4
DEFAULT_CONCURRENCY = 1
DEFAULT_ARGS = {
    "owner": "unic",
    "depends_on_past": False,
    "on_failure_callback": Failure.task_on_failure_callback,
    "on_retry_callback": Slack.notify_task_retry,
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
}
DEFAULT_PARAMS = {
    "branch": Param("master", type="string"),
    "version": Param("latest", type="string")
}

# Spark jobs config
CONFIG_FILE = "config/prod.conf"
SPARK_FAILURE_MSG = "Spark job failed"
JAR = 's3a://spark-prd/jars/unic-etl-{{ params.branch }}.jar'
MASTER_JAR = 's3a://spark-prd/jars/unic-etl-master.jar'
DEFAULT_MULTIPLE_MAIN_METHODS = False
V4_SUBZONES = ["raw", "curated", "released", "enriched"]  # Subzones using ETL v4

# Connection config
MINIO_CONN_ID = "minio"
YELLOW_MINIO_CONN_ID = "yellow_minio"
GREEN_MINIO_CONN_ID = "green_minio"

# Date config
DATE = '{{ data_interval_end | ds }}'
UNDERSCORE_DATE = '{{ data_interval_end | ds | replace("-", "_") }}'

# Version config
VERSION = '{{ params.version }}'
UNDERSCORE_VERSION = '{{ params.version | replace("-", "_") }}'

# Zones
CATALOG_ZONE = "yellow"

# Catalog config
CATALOG_BUCKET = "yellow-prd"

# Released config
RELEASED_BUCKET = "green-prd"
RELEASED_PREFIX = "released"

# Published config
PUBLISHED_BUCKET = "green-prd"
PUBLISHED_PREFIX = "published"

# QA tests config
QA_TEST_MAIN_CLASS = "bio.ferlab.ui.etl.qa.Main"
QA_TEST_CLUSTER_TYPE = "xsmall"
QA_TEST_SPARK_FAILURE_MSG = "Spark test job failed"
QA_TEST_RETRIES = 0

# Optimization config
OPTIMIZATION_MAIN_CLASS = "bio.ferlab.ui.etl.optimization.DeltaTableOptimization"
OPTIMIZATION_CLUSTER_TYPE = "small"
OPTIMIZATION_SPARK_FAILURE_MSG = "Spark optimization job failed"
OPTIMIZATION_RETRIES = 0
