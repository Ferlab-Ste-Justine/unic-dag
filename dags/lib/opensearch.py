from enum import Enum

from airflow.models import Variable


class OpensearchEnv(Enum):
    QA = 'qa'
    PROD = 'prod'

os_port = '9200'
os_credentials_username_key='username'
os_credentials_password_key='password'
os_cert_filename = 'ca.crt'

# Opensearch prod configs
os_prod_url = 'https://workers.opensearch.unic.sainte-justine.intranet'
os_prod_credentials_secret ='opensearch-dags-credentials'
os_prod_cert_secret='unic-prod-opensearch-ca-certificate'
os_prod_username = Variable.get('os_prod_username', None)
os_prod_password = Variable.get('os_prod_password', None)
os_prod_cert = Variable.get('os_prod_ca_certificate', None)
os_prod_cert_path = '/tmp/ca/os/prod/'

# Opensearch qa configs
os_qa_url = 'https://workers.opensearch.qa.unic.sainte-justine.intranet'
os_qa_credentials_secret ='opensearch-qa-dags-credentials'
os_qa_cert_secret ='unic-prod-opensearch-qa-ca-certificate'
os_qa_username = Variable.get('os_qa_username', None)
os_qa_password = Variable.get('os_qa_password', None)
os_qa_cert= Variable.get('os_qa_ca_certificate', None)
os_qa_cert_path = '/tmp/ca/os/qa/'