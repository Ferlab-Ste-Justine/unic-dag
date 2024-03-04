from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from operators.postgresca import PostgresCaOperator
from typing import List, Dict
import subprocess
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

    def execute(self, **kwargs):
        s3_hook = S3Hook(aws_conn_id=self.minio_conn_id)

        filedata = {}

        for table in self.table_copy_conf:
            s3_bucket = table['src_s3_bucket']
            s3_key = table['src_s3_key']
            postgres_schema = table['dts_postgres_schema']
            postgres_tablename = table['dts_postgres_tablename']
            local_file = NamedTemporaryFile(suffix='.csv')

            s3_obj = s3_hook.get_key(key=s3_key, bucket_name=s3_bucket)
            s3_obj.download_fileobj(local_file)
            local_file.flush()
            local_file.seek(0)

            filedata[f"{postgres_schema}.{postgres_tablename}"] = local_file

        with NamedTemporaryFile("w+") as sql:
            subprocess.run(["echo", "BEGIN;"], stdout=sql)

            for tablename, file in filedata.items():
                subprocess.run(["echo", f"TRUNCATE {tablename};"], stdout=sql)
                subprocess.run(["echo", f"COPY {tablename} FROM '{file.name}' DELIMITER ',' CSV HEADER;"], stdout=sql)

            subprocess.run(["echo", "COMMIT;"], stdout=sql)
            sql.flush()
            sql.seek(0)

            self.sql = sql.name[1:]

            super().execute(**kwargs)

        for file in filedata.values():
            file.close()




