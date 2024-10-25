from enum import Enum

from airflow.models import Variable


class PostgresEnv(Enum):
    DEV = 'dev'
    PROD = 'prod'


def unic_postgres_vlan2_conn_id(env: PostgresEnv) -> str:
    return f'unic_{env.value}_postgresql_vlan2_rw'


postgres_bi_conn_id = 'postgresql_bi_rw'

postgres_vlan2_ca_path = '/tmp/ca/'  # Corresponds to path in postgres vlan2 connection string
postgres_bi_ca_path = '/tmp/ca/bi/'  # Corresponds to path in postgres bi connection string

postgres_ca_filename = 'ca.crt'  # Corresponds to filename in postgres connection string
postgres_vlan2_ca_cert = Variable.get('postgres_vlan2_ca_certificate', None)
postgres_bi_ca_cert = Variable.get('postgres_ca_certificate', None)


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
