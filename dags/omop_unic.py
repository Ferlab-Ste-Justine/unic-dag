"""
UNIC OMOP. We will define a schedule in the future.
"""

CONFIG = {"steps": [{
    "destination_zone": "yellow",
    "destination_subzone": "omop",
    "main_class": "bio.ferlab.ui.etl.yellow.omop.Main",
    "multiple_main_methods": True,
    "datasets": [
        {"dataset_id": "omop_location", "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": []},
        {"dataset_id": "omop_person"  , "cluster_type": "medium", "run_type": "default", "pass_date": False, "dependencies": ["omop_location"]},
    ]
}]}