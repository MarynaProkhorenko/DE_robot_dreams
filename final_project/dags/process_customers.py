from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

from schemas import customers_bronze_schema, customers_silver_schema
from queries import populate_silver_customers

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 3,
}

PROJECT_ID = Variable.get("PROJECT_ID")
BUCKET = Variable.get("BUCKET")
BRONZE_DATASET = Variable.get("BRONZE_DATASET")
SILVER_DATASET = Variable.get("SILVER_DATASET")
GOLD_DATASET = Variable.get("GOLD_DATASET")


with DAG(
    dag_id="process_customers",
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
        source_objects=["customers/*.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{BRONZE_DATASET}.customers",
        schema_fields=customers_bronze_schema,
        write_disposition="WRITE_TRUNCATE",
        source_format="CSV",
        skip_leading_rows=1,
    )
    create_silver = BigQueryCreateEmptyTableOperator(
        task_id="create_silver_table",
        dataset_id="silver",
        table_id="customers",
        schema_fields=customers_silver_schema,
    )
    copy_to_silver = BigQueryInsertJobOperator(
        task_id="copy_to_silver",
        configuration={
            "query": {
                "query": populate_silver_customers,
                "useLegacySql": False,
            }
        },
    )

    start >> load_bronze >> create_silver >> copy_to_silver
