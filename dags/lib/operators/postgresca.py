import subprocess
from typing import Sequence

from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.operators.postgres import PostgresOperator


class PostgresCaOperator(PostgresOperator):
    """
    Execute SQL in Postgres with CA certificate

    :param postgres_conn_id: The connection ID used to connect to Postgres
    :param ca_path: Filepath where ca certificate file will be located
    :param ca_filename: Filename where ca certificate file will be written (.crt)
    :param ca_cert: Ca certificate
    """
    template_fields: Sequence[str] = (*PostgresOperator.template_fields, 'skip', 'postgres_conn_id')
    def __init__(
            self,
            postgres_conn_id: str,
            ca_path: str,
            ca_filename: str,
            ca_cert: str,
            skip: bool = False,
            **kwargs) -> None:
        # postgres_conn_id has to be rendered before super class is instantiated
        self.postgres_conn_id = self.render_template(postgres_conn_id, kwargs)
        self.ca_path = ca_path
        self.ca_filename = ca_filename
        self.ca_cert = ca_cert
        self.skip = skip
        super().__init__(postgres_conn_id=self.postgres_conn_id, **kwargs)

    def execute(self, **kwargs):
        if self.skip:
            raise AirflowSkipException()

        self.load_cert()
        super().execute(**kwargs)

    def load_cert(self):
        subprocess.run(["mkdir", "-p", self.ca_path])

        with open(self.ca_path + self.ca_filename, "w") as outfile:
            outfile.write(self.ca_cert)
