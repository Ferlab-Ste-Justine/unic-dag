"""
General dag bag test cases
"""

def test_dags_have_no_import_errors(dag_bag):
    assert not dag_bag.import_errors, f"Import errors found: {dag_bag.import_errors}"
