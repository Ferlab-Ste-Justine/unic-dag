from airflow.decorators import task_group

from lib.operators.postgresca import PostgresCaOperator
from lib.postgres import postgres_bi_ca_path, postgres_ca_filename, postgres_bi_ca_cert, skip_task, postgres_bi_conn_id


@task_group(group_id="create_tables")
def create_tables(sql_config: dict, conn_id: str = postgres_bi_conn_id, ca_path: str = postgres_bi_ca_path,
                  ca_cert: str = postgres_bi_ca_cert):
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
            ca_filename=postgres_ca_filename,
            ca_cert=ca_cert,
            skip=skip_task(table_name))

        jobs[table_name] = {'task': job, 'dependencies': table_config['dependencies']}

    for table_name, job in jobs.items():
        for dependency in job['dependencies']:
            jobs[dependency]['task'] >> job['task']
