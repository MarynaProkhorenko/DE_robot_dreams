from typing import List, Dict, Any
from urllib.parse import urljoin

import requests

from lesson02.ht_template.job1.main import AUTH_TOKEN

API_URL = 'https://fake-api-vycpfa6oca-uc.a.run.app/'
ENDPOINT_URL = urljoin(API_URL, 'sales')


def get_sales(date: str) -> List[Dict[str, Any]]:
    """
    Get data from sales API for specified date.

    :param date: data retrieve the data from
    :return: list of records
    """
    # total pages amount is not known from API,
    # so this loop is used to collect data from all available pages
    page: int = 1
    sales_data: list = []
    while True:
        response = requests.get(
            url=ENDPOINT_URL,
            params={'date': date, 'page': page},
            headers={'Authorization': AUTH_TOKEN},
        )
        if response.ok:
            page_data = response.json()
            sales_data += page_data
            page += 1
        else:  # failed response means that page doesn't exist
            break

    return sales_data
