"""
Airflow Datasets for cross-DAG data-aware scheduling.
"""
from airflow.datasets import Dataset

anonymized_unic_patient_index = Dataset("s3a://yellow-prd/anonymized/unic/patient_index")
