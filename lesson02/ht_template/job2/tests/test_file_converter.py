import json
import os
import shutil
import unittest
from unittest import TestCase

from lesson02.ht_template.job2.bll.file_converter import convert_json_to_avro


class TestConvertJsonToAvro(TestCase):
    def test_convert_json_to_avro(self):
        # Arrange
        test_dir = 'test_results'
        os.mkdir(test_dir)
        input_dir = os.path.join(test_dir, 'input')
        output_dir = os.path.join(test_dir, 'output')
        os.mkdir(input_dir)
        input_filename = 'test_file.json'
        output_filename = input_filename.replace('.json', '.avro')
        with open(os.path.join(input_dir, input_filename), 'w') as f:
            data = [{'client': 'a', 'purchase_date': 'b', 'product': 'c', 'price': 1}]
            json.dump(data, f)

        # Act
        convert_json_to_avro(input_dir, output_dir)

        # Assert
        output_dir_content = os.listdir(output_dir)
        assert output_dir_content == [output_filename]

        # Clean up
        shutil.rmtree(test_dir)


if __name__ == '__main__':
    unittest.main()
