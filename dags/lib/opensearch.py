from enum import Enum

class OpensearchEnv(Enum):
    QA = 'qa'
    PROD = 'prod'

os_port = '9200'
os_credentials_username='username'
os_credentials_password='password'

# Opensearch prod configs
os_prod_url = 'https://workers.opensearch.unic.sainte-justine.intranet'
os_prod_credentials ='opensearch-dags-credentials'
os_prod_cert='unic-prod-opensearch-ca-certificate'

# Opensearch qa configs
os_qa_url = 'https://workers.opensearch.qa.unic.sainte-justine.intranet'
os_qa_credentials ='opensearch-qa-dags-credentials'
os_qa_cert='unic-prod-opensearch-qa-ca-certificate'