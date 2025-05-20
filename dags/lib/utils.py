import re


def sanitize_string(string: str, replace_by: str):
    """
    Replace all special character in a string into another character
    :param string: string to be sanitized
    :param replace_by: replacement character
    :return: sanitized string
    """
    return re.sub("[^a-zA-Z0-9 ]", replace_by, string)


def extract_table_name(dataset_id: str, resource: str) -> str:
    """
    Extracts the table name from a dataset_id string using the known prefix (resource name).

    Args:
        dataset_id (str): The full dataset id (e.g. 'published_bronchiolite_consultation_complication').
        resource (str): The known resource prefix (e.g. 'bronchiolite').

    Returns:
        str: The extracted table name (e.g. 'consultation_complication').
    """
    if resource in dataset_id:
        # Find the prefix in the string and cut everything before and including it
        remaining = dataset_id.split(resource, 1)[1]
    else:
        # Try to extract the table name by removing first two parts of the string
        remaining = dataset_id.split('_', 2)[-1]

    # Remove leading underscore if present
    return remaining.lstrip('_')
