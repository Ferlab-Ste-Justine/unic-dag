from enum import Enum

from airflow.decorators import task
from lib.operators.postgresca import PostgresCaOperator
from lib.postgres import postgres_bi_ca_path, postgres_ca_filename, postgres_bi_ca_cert, postgres_bi_conn_id


class PostgresResource(Enum):
    SCHEMA = "schema"
    TYPES = "types"
    INDEXES = "indexes"

def create_resource(postgres_resource: PostgresResource, sql_config: dict, conn_id: str = postgres_bi_conn_id,
                    ca_path: str = postgres_bi_ca_path, ca_cert: str = postgres_bi_ca_cert) -> PostgresCaOperator:
    return PostgresCaOperator(
        task_id=f"create_{sql_config[postgres_resource.value]['name']}_{postgres_resource.value}",
        postgres_conn_id=conn_id,
        sql=sql_config[postgres_resource.value][f'postgres_{postgres_resource.value}_creation_sql_path'],
        ca_path=ca_path,
        ca_filename=postgres_ca_filename,
        ca_cert=ca_cert
    )

@task(task_id='get_dict_current_version')
def get_dict_current_version(postgres_resource: PostgresResource, sql_config: dict, conn_id: str = conn_id,
                    ca_path: str = postgres_vlan2_ca_path, ca_cert: str = postgres_vlan2_ca_cert) -> PostgresCaOperator:
    return PostgresCaOperator(
        task_id=f"create_{sql_config[postgres_resource.value]['name']}_{postgres_resource.value}",
        postgres_conn_id=conn_id,
        sql=sql_config[postgres_resource.value][f'postgres_{postgres_resource.value}_creation_sql_path'],
        ca_path=ca_path,
        ca_filename=postgres_ca_filename,
        ca_cert=ca_cert
    )