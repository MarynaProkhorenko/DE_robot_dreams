from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCreateEmptyTableOperator
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryExecuteQueryOperator
)

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}


PROJECT_ID = 'de-course-427113'
BUCKET = 'fin_project_raw_bucket'
BRONZE_DATASET = 'bronze'
SILVER_DATASET = 'silver'
GOLD_DATASET = "gold"


with DAG(
    dag_id='process_user_profiles',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    start = EmptyOperator(
        task_id='start',
    )
    create_bronze_table = BigQueryCreateExternalTableOperator(
        task_id='create_bronze_table',
        destination_project_dataset_table=f'{PROJECT_ID}.{BRONZE_DATASET}.user_profiles',
        bucket=BUCKET,
        source_objects=['user_profiles/*.json'],
        schema_fields=[
            {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'full_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'birth_date', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'phone_number', 'type': 'STRING', 'mode': 'NULLABLE'}
        ],
        source_format='NEWLINE_DELIMITED_JSON',
    )

    transform_and_load_silver = BigQueryExecuteQueryOperator(
        task_id='transform_and_load_silver',
        sql=f'''
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{SILVER_DATASET}.user_profiles`
        AS
        SELECT DISTINCT
            email,
            SPLIT(full_name, ' ')[SAFE_OFFSET(0)] AS first_name,
            SPLIT(full_name, ' ')[SAFE_OFFSET(1)] AS last_name,
            state,
            PARSE_DATE('%Y-%m-%d', birth_date) AS birth_date,
            phone_number
        FROM
            `{PROJECT_ID}.{BRONZE_DATASET}.user_profiles`;
        ''',
        use_legacy_sql=False,
    )
    end = EmptyOperator(
        task_id='end',
    )

    start >> create_bronze_table >> transform_and_load_silver >> end
