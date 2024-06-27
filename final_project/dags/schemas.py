users_profiles_bronze_schema = [
            {"name": "email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "full_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "state", "type": "STRING", "mode": "NULLABLE"},
            {"name": "birth_date", "type": "STRING", "mode": "NULLABLE"},
            {"name": "phone_number", "type": "STRING", "mode": "NULLABLE"}
        ]

sales_bronze_schema = [
            {"name": "CustomerId", "type": "STRING", "mode": "NULLABLE"},
            {"name": "PurchaseDate", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Product", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Price", "type": "STRING", "mode": "NULLABLE"},
        ]

sales_silver_schema = [
            {"name": "client_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "purchase_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "price", "type": "FLOAT64", "mode": "NULLABLE"},
        ]

customers_bronze_schema = [
            {"name": "Id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "FirstName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "LastName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "RegistrationDate", "type": "STRING", "mode": "NULLABLE"},
            {"name": "State", "type": "STRING", "mode": "NULLABLE"},
        ]

customers_silver_schema = [
            {"name": "client_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "first_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "last_name", "type": 'STRING', 'mode': 'NULLABLE'},
            {"name": "email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "registration_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "state", "type": "STRING", "mode": "NULLABLE"},
        ]

enrich_users_schema = [
            {"name": "client_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "first_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "last_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "registration_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "state", "type": "STRING", "mode": "NULLABLE"},
            {"name": "birth_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "phone_number", "type": "STRING", "mode": "NULLABLE"},
        ]
