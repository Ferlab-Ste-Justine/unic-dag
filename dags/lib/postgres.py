from airflow.models import Variable

postgres_ca_path = '/tmp/ca/bi/'  # Corresponds to path in postgres connection string
postgres_ca_filename = 'ca.crt'  # Corresponds to filename in postgres connection string
postgres_ca_cert = Variable.get('postgres_ca_certificate', None)

def skip_task(table_name: str) -> str:
    """
    Skip task if table_name is not in the list of tables passed as parameter to the DAG
    """
    return f"{{% if '{table_name}' in params.tables %}}{{% else %}}yes{{% endif %}}"


def drop_table(table_name: str) -> str:
    """
    Generate drop table statement for the given table_name.
    """
    return f"DROP TABLE IF EXISTS {table_name};"
