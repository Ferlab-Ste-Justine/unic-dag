from enum import Enum

class OpensearchEnv(Enum):
    QA = 'qa'
    PROD = 'prod'

os_port = '9200'

# Opensearch prod configs
os_prod_url = 'https://workers.opensearch.unic.sainte-justine.intranet'
os_prod_credentials ='opensearch-dags-credentials',
os_prod_username='OS_USERNAME',
os_prod_password='OS_PASSWORD',
os_prod_cert='unic-prod-opensearch-ca-certificate',

# Opensearch qa configs
os_qa_url = 'https://workers.opensearch.qa.unic.sainte-justine.intranet'
os_qa_credentials ='opensearch-qa-dags-credentials',
os_qa_username='OS_QA_USERNAME',
os_qa_password='OS_QA_PASSWORD',
os_qa_cert='unic-prod-opensearch-qa-ca-certificate',