from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from dags.operators.postgresca import PostgresCaOperator
import subprocess
from tempfile import NamedTemporaryFile


class CopyCsvToPostgres(PostgresCaOperator):
    """
    Copy CSV Files from Minio to Potgesql

    :param _: _
    """
    def __init__(self,**kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, **kwargs):

        tables = ['t1', 't2', 't3']
        schema = "indicateurs_sip"
        filedata = {t: NamedTemporaryFile(suffix='.csv') for t in tables}

        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        s3_obj = s3_hook.get_key(key=self.s3_key, bucket_name=self.s3_bucket)

        for file in filedata.values():
            s3_obj.download_fileobj(file)
            file.flush()
            file.seek(0)

        with NamedTemporaryFile("w+") as sql:
            subprocess.run(["echo", "BEGIN;"], stdout=sql)

            for k, f in filedata.items():
                subprocess.run(["echo", f"TRUNCATE {schema}.{k};"], stdout=sql)
                subprocess.run(["echo", f"COPY {schema}.{k} FROM '{f.name}' DELIMITER ',' CSV HEADER;"], stdout=sql)

                subprocess.run(["echo", "COMMIT;"], stdout=sql)

                # command has finished running, let's check the file
                sql.flush()
                sql.seek(0)
                print(sql.read())
                # hello world

        for file in filedata.values():
            file.close()

        super().execute(**kwargs)




