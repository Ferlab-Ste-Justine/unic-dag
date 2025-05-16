from airflow.decorators import task_group

from lib.operators.postgresca import PostgresCaOperator
from lib.postgres import drop_table, skip_task, POSTGRES_BI_CA_PATH, POSTGRES_CA_FILENAME, POSTGRES_BI_CA_CERT, \
    POSTGRES_BI_CONN_ID


@task_group(group_id="drop_tables")
def drop_tables(sql_config: dict, conn_id: str = POSTGRES_BI_CONN_ID, ca_path: str = POSTGRES_BI_CA_PATH, ca_cert: str = POSTGRES_BI_CA_CERT):
    """
    Drop tables task group.
    """
    [PostgresCaOperator(
        task_id=f"drop_{table_config['name']}_table",
        postgres_conn_id=conn_id,
        sql=drop_table(schema_name=sql_config['schema']['name'], table_name=table_config['name']),
        ca_path=ca_path,
        ca_filename=POSTGRES_CA_FILENAME,
        ca_cert=ca_cert,
        skip=skip_task(table_config['name'])
    ) for table_config in sql_config['tables']]
