import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook

BASE_DIR = os.environ.get("BASE_DIR")


def call_jobs(logical_date,  **kwargs) -> None:
    task_id = kwargs['task'].task_id
    task_date = str(logical_date)[:10]
    headers: dict = {"Content-Type": "application/json"}

    if task_id == "extract_data_from_api":
        body = {
            "date": task_date,
            "raw_dir": os.path.join(BASE_DIR, "raw", "sales", task_date)
        }
    else:
        body = {
            "stg_dir": os.path.join(BASE_DIR, "stg", "sales", task_date),
            "raw_dir": os.path.join(BASE_DIR, "raw", "sales", task_date)
        }
    hook = HttpHook(
        method='POST',
        http_conn_id=f'{task_id}_connection'
    )
    response = hook.run(json=body, headers=headers)
    if response.status_code != 201:
        raise Exception(response.text)


with DAG(
        dag_id="process_sales",
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
        python_callable=call_jobs,
    )

    convert_to_avro = PythonOperator(
        task_id="convert_to_avro",
        python_callable=call_jobs,
        provide_context=True,
    )

    extract_data_from_api >> convert_to_avro
