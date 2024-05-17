import unittest

import requests_mock

from lesson02.ht_template.job1.dal.sales_api import get_sales, ENDPOINT_URL


class TestGetSales(unittest.TestCase):
    def test_get_sales(self):
        # Arrange
        with requests_mock.Mocker() as mocker:
            mocker.get(url=ENDPOINT_URL, status_code=404)

        # Act
        actual_response = get_sales('2023-01-01')

        # Assert
        self.assertEqual(actual_response, [])


if __name__ == '__main__':
    unittest.main()
