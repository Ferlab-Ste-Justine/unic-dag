from enum import Enum

from airflow.decorators import task
from lib.operators.postgresca import PostgresCaOperator
from lib.postgres import POSTGRES_BI_CA_PATH, POSTGRES_CA_FILENAME, POSTGRES_BI_CA_CERT, POSTGRES_BI_CONN_ID


class PostgresResource(Enum):
    SCHEMA = "schema"
    TYPES = "types"
    INDEXES = "indexes"

def create_resource(postgres_resource: PostgresResource, sql_config: dict, conn_id: str = POSTGRES_BI_CONN_ID,
                    ca_path: str = POSTGRES_BI_CA_PATH, ca_cert: str = POSTGRES_BI_CA_CERT) -> PostgresCaOperator:
    return PostgresCaOperator(
        task_id=f"create_{sql_config[postgres_resource.value]['name']}_{postgres_resource.value}",
        postgres_conn_id=conn_id,
        sql=sql_config[postgres_resource.value][f'postgres_{postgres_resource.value}_creation_sql_path'],
        ca_path=ca_path,
        ca_filename=POSTGRES_CA_FILENAME,
        ca_cert=ca_cert
    )