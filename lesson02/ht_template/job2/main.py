"""
This file contains the controller that accepts command via HTTP
and trigger business logic layer
"""
from flask import Flask, request
from flask import typing as flask_typing

from lesson02.ht_template.job2.bll.file_converter import convert_json_to_avro, convert_json_to_csv

app = Flask(__name__)


@app.route('/', methods=['POST'])
def main() -> flask_typing.ResponseReturnValue:
    """
    Controller that accepts command via HTTP and
    trigger business logic layer

    Proposed POST body in JSON:
    {
      "raw_dir": "/path/to/my_dir/raw/sales/2022-08-09"
      "stg_dir": "/path/to/my_dir/stg/sales/2022-08-09"
    }
    """
    input_data: dict = request.json
    raw_dir = input_data.get('raw_dir')
    stg_dir = input_data.get('stg_dir')

    if not raw_dir or not stg_dir:
        return {
            "message": "raw_dir and stg_dir parameters must be provided",
        }, 400

    convert_json_to_avro(dir_with_json=raw_dir, dir_with_avro=stg_dir)

    return {
        "message": "Data recorded successfully",
    }, 201


@app.route("/csv", methods=['POST'])
def transform_to_csv():
    input_data: dict = request.json
    raw_dir = input_data.get('raw_dir')
    stg_dir = input_data.get('stg_dir')

    if not raw_dir or not stg_dir:
        return {
            "message": "raw_dir and stg_dir parameters must be provided",
        }, 400

    convert_json_to_csv(dir_with_json=raw_dir, dir_with_avro=stg_dir)

    return {
        "message": "Data recorded successfully",
    }, 201


if __name__ == "__main__":
    app.run(debug=True, host="localhost", port=8082)
    