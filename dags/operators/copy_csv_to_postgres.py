import csv

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from operators.postgresca import PostgresCaOperator
from typing import List, Dict
from tempfile import NamedTemporaryFile

class CopyCsvToPostgres(PostgresCaOperator):
    """
    Copy CSV Files from Minio to Postgresql

    :param table_copy_conf: List of dicts containing configuration values for each table to copy from minio to postgres
    in a single transaction. Dictionaries should contain the following values: src_s3_bucket, src_s3_key,
    dts_postgres_schema, dts_postgres_tablename.
    :param minio_conn_id: s3 conn URI
    """
    def __init__(
            self,
            table_copy_conf: List[Dict[str, str]],
            minio_conn_id: str,
            **kwargs) -> None:
        super().__init__(
            sql=None,
            **kwargs
        )
        self.table_copy_conf = table_copy_conf
        self.minio_conn_id = minio_conn_id
        self.postgres_conn_id = kwargs.get("postgres_conn_id")

    def execute(self, **kwargs):
        super().load_cert()

        s3_hook = S3Hook(aws_conn_id=self.minio_conn_id)
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        filedata = {}

        for table in self.table_copy_conf:
            s3_bucket = table['src_s3_bucket']
            s3_key = table['src_s3_key']
            postgres_schema = table['dts_postgres_schema']
            postgres_tablename = table['dts_postgres_tablename']
            local_file = NamedTemporaryFile(suffix='.csv')

            s3_conn = s3_hook.get_key(key=s3_key, bucket_name=s3_bucket)
            s3_conn.download_fileobj(local_file)
            local_file.flush()
            local_file.seek(0)

            filedata[f"{postgres_schema}.{postgres_tablename}"] = local_file

        pg_conn = pg_hook.get_conn()

        with pg_conn.cursor() as cur:
            for tablename, file in filedata.items():
                cur.execute(f"TRUNCATE {tablename};")

                with open(file.name, 'rt') as temp_file:
                    cols = csv.DictReader(temp_file, delimiter=',').fieldnames
                    format_cols = ', '.join(cols)

                    print(format_cols)

                cur.copy_expert(f"COPY {tablename} ({format_cols}) FROM stdin DELIMITER ',' CSV HEADER;", file)

            pg_conn.commit()

        pg_conn.close()

        for file in filedata.values():
            file.close()




