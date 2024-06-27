import csv
import logging
from tempfile import NamedTemporaryFile
from typing import List

import psycopg2
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql

from lib.config import root
from lib.operators.postgresca import PostgresCaOperator


class UpsertCsvToPostgres(PostgresCaOperator):
    """
    Upsert a CSV file from S3 to a Postgresql table.

    :param s3_bucket:           Bucket name of the Excel source file
    :param s3_key:              Key of the Excel source file
    :param s3_conn_id:          S3 connection ID
    :param postgres_conn_id     Postgres connection ID
    :param postgres_ca_path     Filepath where ca certificate file will be located
    :param postgres_ca_filename Filename where ca certificate file will be written (.crt)
    :param postgres_ca_cert     CA certificate
    :param schema_name          Postgres schema name
    :param table_name           Postgres table name
    :param table_schema_path    Path where the create table query is located
    :param primary_keys         List of table primary keys used for the upsert
    :param csv_sep              Separator of the CSV file, defaults to ","
    :param skip:                True to skip the task, defaults to False (task is not skipped)
    :return:
    """

    def __init__(
            self,
            s3_bucket: str,
            s3_key: str,
            s3_conn_id: str,
            postgres_conn_id: str,
            postgres_ca_path: str,
            postgres_ca_filename: str,
            postgres_ca_cert: str,
            schema_name: str,
            table_name: str,
            table_schema_path: str,
            primary_keys: List[str],
            csv_sep: str = ",",
            skip: bool = False,
            **kwargs) -> None:
        super().__init__(
            sql=None,
            ca_path=postgres_ca_path,
            ca_filename=postgres_ca_filename,
            ca_cert=postgres_ca_cert,
            **kwargs
        )
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_conn_id = s3_conn_id
        self.schema_name = schema_name
        self.table_name = table_name
        self.table_schema_path = f"{root}/{table_schema_path}"
        self.primary_keys = primary_keys
        self.csv_sep = csv_sep
        self.postgres_conn_id = postgres_conn_id
        self.skip = skip

    def execute(self, **kwargs):
        if self.skip:
            raise AirflowSkipException()

        super().load_cert()

        s3 = S3Hook(aws_conn_id=self.s3_conn_id)
        psql = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        # Download CSV file to upsert
        local_file = NamedTemporaryFile(suffix='.csv')
        s3_transfer = s3.get_key(key=self.s3_key, bucket_name=self.s3_bucket)
        s3_transfer.download_fileobj(local_file)
        local_file.flush()
        local_file.seek(0)

        with open(local_file.name, 'rt') as temp_file:
            columns = csv.DictReader(temp_file, delimiter=self.csv_sep).fieldnames
            update_columns = [col for col in columns if col not in self.primary_keys]

        # Generate create temp table query
        staging_table_name = f"{self.table_name}_staging"
        with open(self.table_schema_path, 'r') as file:
            create_table_query = file.read() \
                .replace("CREATE TABLE", "CREATE TEMP TABLE") \
                .replace(f"{self.schema_name}.{self.table_name}", staging_table_name)

        # Generate copy query
        copy_query = sql.SQL("COPY {staging_table} ({columns}) FROM STDIN DELIMITER {sep} CSV HEADER").format(
            staging_table=sql.Identifier(staging_table_name),
            sep=sql.Literal(self.csv_sep),
            columns=sql.SQL(', ').join(map(sql.Identifier, columns)))

        # Generate upsert query
        upsert_query = sql.SQL("""
        INSERT INTO {target_table} ({columns})
        SELECT {columns} FROM {staging_table}
        ON CONFLICT ({primary_keys})
        """).format(
            target_table=sql.Identifier(self.schema_name, self.table_name),
            columns=sql.SQL(', ').join(map(sql.Identifier, columns)),
            staging_table=sql.Identifier(staging_table_name),
            primary_keys=sql.SQL(", ").join(map(sql.Identifier, self.primary_keys)),
        )

        conflict_statement = sql.SQL("DO NOTHING") if not update_columns else sql.SQL("DO UPDATE SET {set}").format(
            set=sql.SQL(", ").join(
                sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(col)) for col in update_columns))

        upsert_on_conflict_query = upsert_query + conflict_statement

        psql_conn = psql.get_conn()
        with psql_conn.cursor() as cur:
            try:
                # Create staging table
                cur.execute(create_table_query)
                cur.execute(sql.SQL("select * from {}").format(sql.Identifier(staging_table_name)))

                # Copy data to staging table
                cur.copy_expert(copy_query, local_file)
                cur.execute(sql.SQL("select * from {}").format(sql.Identifier(staging_table_name)))

                # Execute upsert
                cur.execute(upsert_on_conflict_query)
                cur.execute(sql.SQL("select * from {}").format(sql.Identifier(self.schema_name, self.table_name)))

                # Drop staging table
                cur.execute(
                    sql.SQL("DROP TABLE {staging_table}").format(staging_table=sql.Identifier(staging_table_name)))

                # Commit all transactions at once
                psql_conn.commit()

            except psycopg2.DatabaseError as error:
                psql_conn.rollback()
                logging.error(f'Failed to upsert CSV to Postgres: {error}')
                raise error

            finally:
                local_file.close()
                cur.close()
                psql_conn.close()
