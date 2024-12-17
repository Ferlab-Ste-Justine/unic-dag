import subprocess

from airflow.providers.postgres.hooks.postgres import PostgresHook

class PostgresCaHook(PostgresHook):
    """
    Interact with Postgres DB.
    :param postgres_conn_id: The connection ID used to connect to Postgres
    :param ca_path: Filepath where ca certificate file will be located
    :param ca_filename: Filename where ca certificate file will be written (.crt)
    :param ca_cert: Ca certificate
    """

    def __init__(
            self,
            postgres_conn_id: str,
            ca_path: str,
            ca_filename: str,
            ca_cert: str,
            *args,
            **kwargs
    ) -> None:
        self.postgres_conn_id = postgres_conn_id
        self.ca_path = ca_path
        self.ca_filename = ca_filename
        self.ca_cert = ca_cert
        super().__init__(postgres_conn_id=self.postgres_conn_id,*args, **kwargs)

    def get_conn(self):
        self.load_cert()

        super().get_conn()

    def load_cert(self):
        subprocess.run(["mkdir", "-p", self.ca_path])

        with open(self.ca_path + self.ca_filename, "w") as outfile:
            outfile.write(self.ca_cert)