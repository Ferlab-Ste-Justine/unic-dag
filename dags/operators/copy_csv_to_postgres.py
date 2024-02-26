from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from dags.operators.postgresca import PostgresCaOperator
import subprocess
from tempfile import NamedTemporaryFile


class CopyCsvToPostgres(PostgresCaOperator):
    """
    Copy CSV Files from Minio to Postgresql

    :param table_copy_conf: List of dicts containing configuration values for each table to copy from minio to postgres
    in a single transaction. dictionary should contain the following values: src_s3_bucket, src_s3_key and dts_postgres_tablename.
    :param minio_conn_id: s3 conn URI
    :param schema: (Optional) Name of Postgres schema
    """
    def __init__(
            self,
            table_copy_conf: list[dict],
            minio_conn_id: str,
            schema: str = "default",
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.table_copy_conf = table_copy_conf
        self.minio_conn_id = minio_conn_id
        self.schema = schema
        self.sql = None

    def execute(self, **kwargs):
        s3_hook = S3Hook(aws_conn_id=self.minio_conn_id)

        filedata = {}

        for table in self.table_copy_conf:
            s3_bucket = table['src_s3_bucket']
            s3_key = table['src_s3_key']
            postgres_tablename = table['dts_postgres_tablename']
            local_file = NamedTemporaryFile(suffix='.csv')

            s3_obj = s3_hook.get_key(key=s3_bucket, bucket_name=s3_key)
            s3_obj.download_fileobj(local_file)
            local_file.flush()
            local_file.seek(0)

            filedata[postgres_tablename] = local_file

        with NamedTemporaryFile("w+") as sql:
            subprocess.run(["echo", "BEGIN;"], stdout=sql)

            for tablename, file in filedata.items():
                subprocess.run(["echo", f"TRUNCATE {self.schema}.{tablename};"], stdout=sql)
                subprocess.run(["echo", f"COPY {self.schema}.{tablename} FROM '{file.name}' DELIMITER ',' CSV HEADER;"], stdout=sql)

            subprocess.run(["echo", "COMMIT;"], stdout=sql)
            sql.flush()
            sql.seek(0)

            self.sql = sql.name

            super().execute(**kwargs)

        for file in filedata.values():
            file.close()




