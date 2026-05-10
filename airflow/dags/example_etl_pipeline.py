from datetime import datetime, timedelta
import logging

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='example_postgres_etl',
    default_args=default_args,
    description='A simple ETL pipeline connecting to PostgreSQL',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example', 'etl'],
)
def postgres_etl_pipeline():
    """
    ### Example Postgres ETL
    This DAG demonstrates a simple ETL pipeline using the TaskFlow API and PostgresHook.
    It extracts data from the `raw_orders` table, transforms it, and loads it into the `processed_orders` table.
    """

    @task()
    def extract_data():
        """Extract data from Postgres raw_orders table."""
        # 'airflow_db' connection is typically the default one pointing to the metadata db
        # Here we use the underlying sqlalchemy connection, but in production, 
        # you'd configure a specific Airflow Connection in the UI for your target database.
        hook = PostgresHook(postgres_conn_id='airflow_db')
        
        # In a real scenario, use an Airflow Connection configured in the UI. 
        # Since we use the metadata DB for our sample data, we can just use sqlalchemy string.
        # However, to be purely idiomatic without extra UI config, we can use the default connection.
        
        sql = "SELECT order_id, customer_name, product_name, quantity, price FROM sample_data.raw_orders;"
        
        # In a real ETL, be careful with large datasets fitting into memory.
        records = hook.get_records(sql)
        logging.info(f"Extracted {len(records)} records.")
        return records

    @task()
    def transform_data(records: list):
        """Transform the extracted records (calculate total amount)."""
        transformed_records = []
        for record in records:
            # record is a tuple: (order_id, customer_name, product_name, quantity, price)
            order_id = record[0]
            customer_name = record[1]
            product_name = record[2]
            quantity = record[3]
            price = float(record[4])  # Ensure price is a float
            
            total_amount = quantity * price
            
            transformed_records.append((order_id, customer_name, product_name, total_amount))
            
        logging.info(f"Transformed {len(transformed_records)} records.")
        return transformed_records

    @task()
    def load_data(transformed_records: list):
        """Load the transformed data into Postgres processed_orders table."""
        hook = PostgresHook(postgres_conn_id='airflow_db')
        
        # We will clear the table first for idempotency in this example
        hook.run("TRUNCATE TABLE sample_data.processed_orders;")
        
        insert_sql = """
            INSERT INTO sample_data.processed_orders 
            (order_id, customer_name, product_name, total_amount) 
            VALUES (%s, %s, %s, %s);
        """
        
        # Using PostgresHook's insert_rows is also possible, but executing many is standard
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        cursor.executemany(insert_sql, transformed_records)
        conn.commit()
        
        logging.info(f"Loaded {len(transformed_records)} records into processed_orders table.")

    # Define task dependencies
    raw_data = extract_data()
    processed_data = transform_data(raw_data)
    load_data(processed_data)

# Instantiate the DAG
etl_dag = postgres_etl_pipeline()
