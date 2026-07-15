"""
Dataset coupling between curated_unic (producer) and warehouse_unic (consumer).
"""
from lib.datasets import anonymized_unic_patient_index


def test_curated_unic_publishes_patient_index_dataset(dag_bag):
    dag = dag_bag.dags["curated_unic"]
    outlets = [uri for task in dag.tasks for uri in (d.uri for d in task.outlets)]
    assert anonymized_unic_patient_index.uri in outlets


def test_warehouse_unic_scheduled_on_patient_index_dataset(dag_bag):
    dag = dag_bag.dags["warehouse_unic"]
    condition = dag.timetable.dataset_condition
    uris = [d.uri for _, d in condition.iter_datasets()]
    assert uris == [anonymized_unic_patient_index.uri]
