import logging
import requests

from airflow.exceptions import AirflowSkipException
from lib.config import es_url

def get_release_id(release_id: str, index: str, increment: bool = True, skip: bool = False) -> str:
    if skip:
        raise AirflowSkipException()

    if release_id:
        logging.info(f'Using release id passed to DAG: {release_id}')
        return release_id

    logging.info(f'No release id passed to DAG. Fetching release id from ES for index {index}.')
    # Fetch current id from ES
    url = f'{es_url}/{index}?&pretty'
    response = requests.get(url)
    logging.info(f'ES response:\n{response.text}')

    # Parse current id
    current_full_release_id = list(response.json())[0]  # {index}_re_00xx
    current_release_id = current_full_release_id.split('_')[-1]  # 00xx
    logging.info(f'Current release id: re_{current_release_id}')

    if increment:
        # Increment current id by 1
        new_release_id = f're_{str(int(current_release_id) + 1).zfill(4)}'
        logging.info(f'New release id: {new_release_id}')
        return new_release_id
    else:
        return f're_{current_release_id}'