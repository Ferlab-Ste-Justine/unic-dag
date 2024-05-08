from lib.operators.postgresca import PostgresCaOperator
from lib.postgres import postgres_ca_path, postgres_ca_filename, postgres_ca_cert


def create_schema(sql_config: dict) -> PostgresCaOperator:
    """
    Create schema in Postgres
    """
    return PostgresCaOperator(
        task_id=f"create_{sql_config['schema']['name']}_schema",
        postgres_conn_id="postgresql_bi_rw",
        sql=sql_config['schema']['postgres_schema_creation_sql_path'],
        ca_path=postgres_ca_path,
        ca_filename=postgres_ca_filename,
        ca_cert=postgres_ca_cert
    )
