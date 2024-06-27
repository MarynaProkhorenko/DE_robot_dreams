from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCreateEmptyTableOperator
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

from schemas import enrich_users_schema
from queries import enrich_users_profiles_query

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
        schema_fields=enrich_users_schema,
    )
    enrich_customers = BigQueryInsertJobOperator(
        task_id="enrich_customers",
        configuration={
            "query": {
                "query": enrich_users_profiles_query,
                "useLegacySql": False,
            }
        },
    )
    end = EmptyOperator(
        task_id="end",
    )
    start >> create_gold_table >> enrich_customers >> end
