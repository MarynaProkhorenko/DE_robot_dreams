from airflow.models import Variable

PROJECT_ID = Variable.get("PROJECT_ID")
BUCKET = Variable.get("BUCKET")
BRONZE_DATASET = Variable.get("BRONZE_DATASET")
SILVER_DATASET = Variable.get("SILVER_DATASET")
GOLD_DATASET = Variable.get("GOLD_DATASET")


populate_silver_sales = f"""
                  SELECT
                      CAST(CustomerId AS STRING) AS client_id,
                      IFNULL(
                          SAFE.PARSE_TIMESTAMP('%Y-%m-%d', PurchaseDate),
                          SAFE.PARSE_TIMESTAMP('%Y-%b-%d', PurchaseDate)
                      ) AS purchase_date,
                      CAST(Product AS STRING) AS product_name,
                      CAST(Price AS FLOAT64) AS price
                  FROM
                      `{PROJECT_ID}.bronze.sales`
                  WHERE
                      SAFE_CAST(Price AS FLOAT64) IS NOT NULL
                  """

populate_silver_customers = f"""
                  CREATE OR REPLACE TABLE `{PROJECT_ID}.silver.customers` AS
                  SELECT
                    DISTINCT 
                    Id AS client_id,
                    FirstName AS first_name,
                    LastName AS last_name,
                    Email AS email,
                    IFNULL(
                       SAFE.PARSE_TIMESTAMP('%Y-%m-%d', RegistrationDate),
                       SAFE.PARSE_TIMESTAMP('%Y-%b-%d', RegistrationDate)
                    ) AS registration_date,
                    State AS state
                  FROM
                      `{PROJECT_ID}.bronze.customers`
                  """

populate_users_profiles = f"""
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
        """

enrich_users_profiles_query = f"""
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
                """
