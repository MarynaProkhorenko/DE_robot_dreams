from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from queries import populate_silver_sales

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
}

PROJECT_ID = Variable.get("PROJECT_ID")
BUCKET = Variable.get("BUCKET")
BRONZE_DATASET = Variable.get("BRONZE_DATASET")
SILVER_DATASET = Variable.get("SILVER_DATASET")


with DAG(
    dag_id="process_sales",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    start = EmptyOperator(
        task_id="start",
    )
    load_bronze = GCSToBigQueryOperator(
        task_id="load_bronze",
        bucket=f"{BUCKET}",
        source_objects=["sales/*.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{BRONZE_DATASET}.sales",
        schema_fields=[
            {"name": "CustomerId", "type": "STRING", "mode": "NULLABLE"},
            {"name": "PurchaseDate", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Product", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Price", "type": "STRING", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
        source_format="CSV",
        skip_leading_rows=1,
    )
    create_partitioning_silver = BigQueryCreateEmptyTableOperator(
        task_id='create_partitioning_silver',
        dataset_id='silver',
        table_id='sales',
        schema_fields=[
            {"name": "client_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "purchase_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "price", "type": "FLOAT64", "mode": "NULLABLE"},
        ],
        time_partitioning={
            "type": "DAY",
            "field": "purchase_date",
        },
    )
    copy_to_silver = BigQueryInsertJobOperator(
        task_id="copy_to_silver",
        configuration={
            "query": {
                "query": populate_silver_sales,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": f"{PROJECT_ID}",
                    "datasetId": f"{SILVER_DATASET}",
                    "tableId": "sales",
                },
                "writeDisposition": "WRITE_APPEND",
            }
        },
    )

    start >> load_bronze >> create_partitioning_silver >> copy_to_silver
