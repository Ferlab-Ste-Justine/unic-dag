import subprocess

from airflow.exceptions import AirflowSkipException

from lib.operators.spark import SparkOperator


class SparkOpenSearchOperator(SparkOperator):
    template_fields = SparkOperator.template_fields + (
        'spark_class',
        'spark_jar',
        'spark_failure_msg',
        'zone',
        'spark_config',
        'skip'
    )
    def __init__(
            self,
            spark_class: str,
            spark_jar: str,
            spark_failure_msg: str,
            zone: str,
            ca_path: str,
            ca_filename: str,
            ca_cert: str,
            spark_config: str = '',
            skip: bool = False,
            **kwargs,
    ) -> None:
        super().__init__(
            spark_class=self.spark_class,
            spark_jar=self.spark_jar,
            spark_failure_msg=self.spark_failure_msg,
            zone=self.zone,
            spark_config=self.spark_config,
            **kwargs
        )
        self.spark_class = spark_class
        self.spark_jar = spark_jar
        self.spark_failure_msg = spark_failure_msg
        self.zone = zone
        self.spark_config = spark_config
        self.ca_path = ca_path
        self.ca_filename = ca_filename
        self.ca_cert = ca_cert
        self.skip = skip

    def execute(self, **kwargs):
        if self.skip:
            raise AirflowSkipException()

        self.load_cert()
        super().execute(**kwargs)

    def load_cert(self):
        subprocess.run(["mkdir", "-p", self.ca_path])

        with open(self.ca_path + self.ca_filename, "w") as outfile:
            outfile.write(self.ca_cert)

        subprocess.run(["sh", "-c", f"keytool -importkeystore -noprompt -srckeystore /etc/pki/java/cacerts -destkeystore /opt/keystores/truststore.p12 -srcstoretype PKCS12 -deststoretype PKCS12 -srcstorepass changeit -storepass changeit && keytool -import -noprompt -keystore /opt/keystores/truststore.p12 -file {self.ca_path}{self.ca_filename} -storepass changeit -alias es"])

