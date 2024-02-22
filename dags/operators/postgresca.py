from airflow.providers.postgres.operators.postgres import PostgresOperator
import subprocess
import os

class PostgresCaOperator(PostgresOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, **kwargs):
        subprocess.run(["mkdir", "-p", "/tmp/ca/bi"])

        echo_arg = os.environ['AIRFLOW_VAR_POSTGRES_CA_CERTIFICATE']

        with open('/tmp/ca/bi/ca.crt', "w") as outfile:
            subprocess.run(["echo", echo_arg], stdout=outfile)

        super().execute(**kwargs)
