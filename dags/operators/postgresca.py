from airflow.providers.postgres.operators.postgres import PostgresOperator
import os


class PostgresCaOperator(PostgresOperator):
    """
    Execute SQL in Postgres with CA certificate

    :param ca_path: Filepath where ca certificate file will be written (.crt)
    :param ca_var: Name of environment variable in cluster containing ca certificate
    """
    def __init__(
            self,
            ca_path: str,
            ca_var: str,
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.ca_path = ca_path
        self.ca_var = ca_var

    def execute(self, **kwargs):
        ca = os.environ.get(self.ca_var)

        with open(self.ca_path, "w") as outfile:
            outfile.write(ca)

        super().execute(**kwargs)
