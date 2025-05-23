from enum import Enum

from airflow.models import Variable

from lib.hooks.postgresca import PostgresCaHook

class PostgresEnv(Enum):
    DEV = 'dev'
    PROD = 'prod'


def unic_postgres_vlan2_conn_id(env: PostgresEnv) -> str:
    return f'unic_{env.value}_postgresql_vlan2_rw'


POSTGRES_BI_CONN_ID = 'postgresql_bi_rw'

POSTGRES_VLAN2_CA_PATH = '/tmp/ca/'  # Corresponds to path in postgres vlan2 connection string
POSTGRES_BI_CA_PATH = '/tmp/ca/bi/'  # Corresponds to path in postgres bi connection string

POSTGRES_CA_FILENAME = 'ca.crt'  # Corresponds to filename in postgres connection string
POSTGRES_VLAN2_CA_CERT = Variable.get('postgres_vlan2_ca_certificate', None)
POSTGRES_BI_CA_CERT = Variable.get('postgres_ca_certificate', None)


def skip_task(table_name: str) -> str:
    """
    Skip task if table_name is not in the list of tables passed as parameter to the DAG
    """
    return f"{{% if '{table_name}' in params.tables %}}{{% else %}}yes{{% endif %}}"


def drop_table(schema_name: str, table_name: str) -> str:
    """
    Generate drop table statement for the given table_name.
    """
    return f"DROP TABLE IF EXISTS {schema_name}.{table_name} CASCADE;"

def get_pg_ca_hook(pg_conn_id: str, ca_path: str = POSTGRES_VLAN2_CA_PATH, ca_filename: str = POSTGRES_CA_FILENAME,
                ca_cert: str = POSTGRES_VLAN2_CA_CERT) -> PostgresCaHook:
    """
    Get the Postgres Hook for the given environment.
    """
    return PostgresCaHook(postgres_conn_id=pg_conn_id, ca_path=ca_path,
                        ca_filename=ca_filename, ca_cert=ca_cert)

