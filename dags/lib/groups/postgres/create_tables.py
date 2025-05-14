from airflow.decorators import task_group

from lib.operators.postgresca import PostgresCaOperator
from lib.postgres import POSTGRES_BI_CA_PATH, POSTGRES_CA_FILENAME, POSTGRES_BI_CA_CERT, skip_task, POSTGRES_BI_CONN_ID


@task_group(group_id="create_tables")
def create_tables(sql_config: dict, conn_id: str = POSTGRES_BI_CONN_ID, ca_path: str = POSTGRES_BI_CA_PATH,
                  ca_cert: str = POSTGRES_BI_CA_CERT):
    """
    Create tables task group.
    """
    jobs = {}
    for table_config in sql_config['tables']:
        table_name = table_config['name']
        job = PostgresCaOperator(
            task_id=f"create_{table_name}_table",
            postgres_conn_id=conn_id,
            sql=table_config['postgres_table_creation_sql_path'],
            ca_path=ca_path,
            ca_filename=POSTGRES_CA_FILENAME,
            ca_cert=ca_cert,
            skip=skip_task(table_name))

        jobs[table_name] = {'task': job, 'dependencies': table_config['dependencies']}

    for table_name, job in jobs.items():
        for dependency in job['dependencies']:
            jobs[dependency]['task'] >> job['task']
