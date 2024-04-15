"""
Tests dal.local_disk.py module
"""
import json
import os
import unittest
from unittest import TestCase

from lesson02.ht_template.job1.dal.local_disk import save_to_disk


class SaveToDiskTestCase(TestCase):
    def test_save_to_disk(self):
        """Test dal.local_disk.save_to_disk function."""
        # Arrange
        json_content = [{'key': 'value'}]
        path = 'test_results'
        filename = 'test_file.json'
        expected_path_to_file = os.path.join(path, filename)

        # Act
        save_to_disk(json_content, path, filename)

        # Assert
        with open(expected_path_to_file) as f:
            file_data = json.load(f)
        self.assertEqual(file_data, json_content)

        # Clean up
        os.remove(expected_path_to_file)
        os.rmdir(path)


if __name__ == '__main__':
    unittest.main()
