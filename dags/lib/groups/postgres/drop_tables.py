from airflow.decorators import task_group
from lib.operators.postgresca import PostgresCaOperator

from lib.postgres import drop_table, skip_task, postgres_ca_path, postgres_ca_filename, postgres_ca_cert


@task_group(group_id="drop_tables")
def drop_tables(sql_config: dict):
    """
    Drop tables task group.
    """
    [PostgresCaOperator(
        task_id=f"drop_{table_config['name']}_table",
        postgres_conn_id="postgresql_bi_rw",
        sql=drop_table(table_config['name']),
        ca_path=postgres_ca_path,
        ca_filename=postgres_ca_filename,
        ca_cert=postgres_ca_cert,
        skip=skip_task(table_config['name'])
    ) for table_config in sql_config['tables']]
