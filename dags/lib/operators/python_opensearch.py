import subprocess
from typing import Callable, Sequence

from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator

class PythonOpenSearchOperator(PythonOperator):
    """
    Execute Python with OpenSearch Connection

    :param: python_callable: The connection ID used to connect to Postgres
    :param ca_path: Filepath where ca certificate file will be located
    :param ca_filename: Filename where ca certificate file will be written (.crt)
    :param ca_cert: Ca certificate
    """
    template_fields: Sequence[str] = (*PythonOperator.template_fields, 'skip', 'python_callable', 'provide_context')
    def __init__(
            self,
            python_callable: Callable,
            ca_path: str,
            ca_filename: str,
            ca_cert: str,
            provide_context:  bool = True,
            skip: bool = False,
            **kwargs) -> None:
        # python_callable has to be rendered before super class is instantiated
        self.python_callable = python_callable
        self.ca_path = ca_path
        self.ca_filename = ca_filename
        self.ca_cert = ca_cert
        self.provide_context = provide_context,
        self.skip = skip
        super().__init__(python_callable=self.python_callable, provide_context=self.provide_context, **kwargs)

    def execute(self, **kwargs):
        if self.skip:
            raise AirflowSkipException()

        self.load_cert()
        super().execute(**kwargs)

    def load_cert(self):
        subprocess.run(["mkdir", "-p", self.ca_path])

        with open(self.ca_path + self.ca_filename, "w") as outfile:
            outfile.write(self.ca_cert)
