from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCreateEmptyTableOperator
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
}

PROJECT_ID = "de-course-427113"
BUCKET = "fin_project_raw_bucket"
BRONZE_DATASET = "bronze"
SILVER_DATASET = "silver"
GOLD_DATASET = "gold"


with DAG(
    dag_id="enrich_user_profiles",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    start = EmptyOperator(
        task_id="start",
    )
    create_gold_table = BigQueryCreateEmptyTableOperator(
        task_id="create_gold_table",
        dataset_id=GOLD_DATASET,
        table_id="user_profiles_enriched",
        schema_fields=[
            {"name": "client_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "first_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "last_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "registration_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "state", "type": "STRING", "mode": "NULLABLE"},
            {"name": "birth_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "phone_number", "type": "STRING", "mode": "NULLABLE"},
        ],
    )
    enrich_customers = BigQueryInsertJobOperator(
        task_id="enrich_customers",
        configuration={
            "query": {
                "query": f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{GOLD_DATASET}.user_profiles_enriched` AS
                SELECT
                    c.client_id,
                    COALESCE(u.first_name, c.first_name) AS first_name,
                    COALESCE(u.last_name, c.last_name) AS last_name,
                    c.email,
                    c.registration_date,
                    COALESCE(u.state, c.state) AS state,
                    u.email AS profile_email,
                    u.birth_date AS birth_date,
                    u.phone_number
                FROM
                    `{PROJECT_ID}.{SILVER_DATASET}.customers` c
                LEFT JOIN
                    `{PROJECT_ID}.{SILVER_DATASET}.user_profiles` u
                ON
                    c.email = u.email
                """,
                "useLegacySql": False,
            }
        },
    )
    end = EmptyOperator(
        task_id="end",
    )
    start >> create_gold_table >> enrich_customers >> end
