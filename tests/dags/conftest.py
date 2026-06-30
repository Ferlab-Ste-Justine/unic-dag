from pathlib import Path

import pytest
from airflow.models import DagBag


DAGS_DIR = Path(__file__).parent.parent / 'dags'

@pytest.fixture(scope='session')
def dag_bag():
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
