import os
from datetime import datetime
from airflow import DAG
from dotenv import load_dotenv
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
load_dotenv()
BUCKET_NAME = os.getenv("BUCKET_NAME")
BASE_DIR = os.getenv("BASE_DIR")
GCS_FOLDER = os.getenv("GCS_BASE_DIR") + "{{ ds }}"
LOCAL_FILE_PATH = os.path.join(BASE_DIR, "stg_csv", "sales", '{{ ds }}.csv')
DESTINATION_FILE_PATH = os.path.join(GCS_FOLDER, '{{ ds }}.csv')


def extract_data(logical_date, **kwargs):
    task_date = str(logical_date)[:10]
    headers: dict = {"Content-Type": "application/json"}

    body = {
        "date": task_date,
        "raw_dir": os.path.join(BASE_DIR, "raw", "sales", task_date)
    }
    hook = HttpHook(method='POST', http_conn_id='extract_data_connection')
    response = hook.run(json=body, headers=headers)
    if response.status_code != 201:
        raise Exception(response.text)


def transform_data(logical_date, **kwargs):
    task_date = str(logical_date)[:10]
    headers: dict = {"Content-Type": "application/json"}
    stg_dir = os.path.join(BASE_DIR, "stg_csv", "sales", task_date),
    body = {
        "stg_dir": stg_dir,
        "raw_dir": os.path.join(BASE_DIR, "raw", "sales", task_date)
    }
    hook = HttpHook(method='POST', http_conn_id='transform_job_connection')
    response = hook.run('/csv', json=body, headers=headers)
    if response.status_code != 201:
        raise Exception(response.text)


with DAG(
        dag_id="process_sales_cloud",
        start_date=datetime(2022, 8, 9),
        end_date=datetime(2022, 8, 11, 2),
        schedule_interval="0 1 * * *",
        tags=['homework'],
        max_active_runs=1,
        catchup=True
) as dag:
    extract_data_from_api = PythonOperator(
        task_id="extract_data_from_api",
        provide_context=True,
        python_callable=extract_data,
    )

    convert_to_csv = PythonOperator(
        task_id="convert_to_csv",
        python_callable=transform_data,
        provide_context=True,
    )
    upload_file_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=LOCAL_FILE_PATH,
        dst=DESTINATION_FILE_PATH,
        bucket=BUCKET_NAME,
    )

    extract_data_from_api >> convert_to_csv >> upload_file_to_gcs
