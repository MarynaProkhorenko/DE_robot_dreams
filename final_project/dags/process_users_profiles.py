from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryExecuteQueryOperator
)
from airflow.models import Variable
from queries import populate_users_profiles

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
}


PROJECT_ID = Variable.get("PROJECT_ID")
BUCKET = Variable.get("BUCKET")
BRONZE_DATASET = Variable.get("BRONZE_DATASET")
SILVER_DATASET = Variable.get("SILVER_DATASET")
GOLD_DATASET = Variable.get("GOLD_DATASET")


with DAG(
    dag_id="process_user_profiles",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    start = EmptyOperator(
        task_id="start",
    )
    create_bronze_table = BigQueryCreateExternalTableOperator(
        task_id="create_bronze_table",
        destination_project_dataset_table=f"{PROJECT_ID}.{BRONZE_DATASET}.user_profiles",
        bucket=BUCKET,
        source_objects=["user_profiles/*.json"],
        schema_fields=[
            {"name": "email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "full_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "state", "type": "STRING", "mode": "NULLABLE"},
            {"name": "birth_date", "type": "STRING", "mode": "NULLABLE"},
            {"name": "phone_number", "type": "STRING", "mode": "NULLABLE"}
        ],
        source_format="NEWLINE_DELIMITED_JSON",
    )

    transform_and_load_silver = BigQueryExecuteQueryOperator(
        task_id='transform_and_load_silver',
        sql=populate_users_profiles,
        use_legacy_sql=False,
    )
    end = EmptyOperator(
        task_id="end",
    )

    start >> create_bronze_table >> transform_and_load_silver >> end
