import json
import os
import pandas as pd

import fastavro

from lesson02.ht_template.bin.utils import recreate_empty_dir


def convert_json_to_avro(dir_with_json: str, dir_with_avro: str) -> None:
    """It converts JSON files from input folder into AVRO files in output folder."""
    recreate_empty_dir(dir_with_avro)
    for file in os.listdir(dir_with_json):
        json_path = os.path.join(dir_with_json, file)
        file_name = os.path.splitext(file)[0]
        avro_path = os.path.join(dir_with_avro, f'{file_name}.avro')

        # Read JSON data
        with open(json_path, 'r', encoding='utf-8') as json_file:
            data = json.load(json_file)

        # Write Avro file
        avro_schema = {
            'type': 'record',
            'name': 'sales',
            'fields': [
                {'name': 'client', 'type': 'string'},
                {'name': 'purchase_date', 'type': 'string'},
                {'name': 'product', 'type': 'string'},
                {'name': 'price', 'type': 'int'}
            ]
        }
        with open(avro_path, 'wb') as avro_file:
            fastavro.writer(avro_file, avro_schema, data)


def convert_json_to_csv(dir_with_json, dir_with_csv):
    """Convert all files from JSON directory to CSV."""
    # Check if directory exists
    if not os.path.exists(dir_with_csv):
        os.makedirs(dir_with_csv)

    # Transform all files
    for file_name in os.listdir(dir_with_json):
        if file_name.endswith('.json'):
            json_file_path = os.path.join(dir_with_json, file_name)
            csv_file_path = os.path.join(dir_with_json, file_name.replace('.json', '.csv'))

            # Read JSON
            with open(json_file_path, 'r', encoding='utf-8') as json_file:
                records = json.load(json_file)

            # Collect data as DF
            df = pd.DataFrame(records)

            # Write CSV
            df.to_csv(csv_file_path, index=False)