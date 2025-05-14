import subprocess
from enum import Enum

from airflow.models import Variable

from lib.postgres import PostgresEnv
from lib.templates.resource_centric import resource_centric_template
from lib.templates.table_centric import table_centric_template
from lib.templates.variable_centric import variable_centric_template


class OpensearchEnv(Enum):
    QA = 'qa'
    PROD = 'prod'

class OpensearchAlias(Enum):
    RESOURCE = 'resource_centric'
    TABLE = 'table_centric'
    VARIABLE = 'variable_centric'


"""
Opensearch environment to Postgres environment mapping
"""
os_env_pg_env_mapping: dict = {
    OpensearchEnv.PROD: PostgresEnv.PROD,
    OpensearchEnv.QA: PostgresEnv.DEV
}

"""
Opensearch index templates
"""
os_templates: dict = {
    OpensearchAlias.RESOURCE.value: resource_centric_template,
    OpensearchAlias.TABLE.value: table_centric_template,
    OpensearchAlias.VARIABLE.value: variable_centric_template
}

"""
Opensearch id columns
"""
os_id_columns: dict = {
    OpensearchAlias.RESOURCE.value: "rs_id",
    OpensearchAlias.TABLE.value: "tab_id",
    OpensearchAlias.VARIABLE.value: "var_id"
}

"""
OpenSearch environment configuration map
"""
os_env_config = {
    OpensearchEnv.PROD.value : {
        'host': 'workers.opensearch.unic.sainte-justine.intranet',
        'port': '9200',
        'username': Variable.get('os_prod_username', None),
        'password': Variable.get('os_prod_password', None),
        'ca_cert': Variable.get('os_prod_ca_certificate', None),
        'ca_path': '/tmp/ca/os/prod/',
        'ca_filename': 'ca.crt'
    },
    OpensearchEnv.QA.value : {
        'host': 'workers.opensearch.qa.unic.sainte-justine.intranet',
        'port': '9200',
        'username': Variable.get('os_qa_username', None),
        'password': Variable.get('os_qa_password', None),
        'ca_cert': Variable.get('os_qa_ca_certificate', None),
        'ca_path': '/tmp/ca/os/qa/',
        'ca_filename': 'ca.crt'
    }
}

def load_cert(env_name: str) -> None:
    """
    Load the os ca-certificate into task for the specified environment.
    """
    os_config = os_env_config.get(env_name)

    subprocess.run(["mkdir", "-p", os_config.get('ca_path')])

    with open(os_config.get('ca_path') + os_config.get('ca_filename'), "w") as outfile:
        outfile.write(os_config.get('ca_cert'))

def get_opensearch_client(env_name: str):
    """
    Get OpenSearch client for the specified environment.
    """
    from opensearchpy import OpenSearch

    os_config = os_env_config.get(env_name)

    host = os_config.get('host')
    port = os_config.get('port')
    auth = (os_config.get('username'), os_config.get('password'))
    ca_certs_path = os_config.get('ca_path') + os_config.get('ca_filename')

    client = OpenSearch(
        hosts = [{'host': host, 'port': port}],
        http_compress = True,
        http_auth = auth,
        use_ssl = True,
        verify_certs = True,
        ssl_assert_hostname = False,
        ssl_show_warn = False,
        ca_certs = ca_certs_path
    )

    return client
