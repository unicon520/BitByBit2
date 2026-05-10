from datetime import datetime
import pandas as pd
# pyrefly: ignore [missing-import]
from airflow.decorators import dag, task
from airflow import Dataset
# pyrefly: ignore [missing-import]
from airflow.providers.postgres.hooks.postgres import PostgresHook

onepa_silver_dataset = Dataset("file:///opt/airflow/data/load/onepa_silver.csv")

@dag(
    dag_id='3_onepa_gold_load',
    schedule=[onepa_silver_dataset], # Automatically runs when Silver completes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['medallion', 'gold', 'onepa', 'load'],
    description='Loads Silver OnePA data into Gold Postgres Table'
)
def onepa_gold_load_pipeline():

    @task()
    def load_to_gold():
        silver_file_path = '/opt/airflow/data/load/onepa_silver.csv'
        
        print(f"Reading Silver data from {silver_file_path}")
        df = pd.read_csv(silver_file_path)
        
        # Handle nan/null values before inserting
        df = df.where(pd.notnull(df), None)
        
        hook = PostgresHook(postgres_conn_id='airflow_db')
        
        setup_sql = """
            CREATE SCHEMA IF NOT EXISTS medallion;
            CREATE TABLE IF NOT EXISTS medallion.onepa_events (
                event_id VARCHAR(50) PRIMARY KEY,
                title TEXT,
                url TEXT,
                outlet TEXT,
                start_date TEXT,
                session_time TEXT,
                registration_open BOOLEAN,
                min_price DECIMAL(10, 2),
                max_price DECIMAL(10, 2),
                source TEXT
            );
            TRUNCATE TABLE medallion.onepa_events;
        """
        hook.run(setup_sql)
        
        insert_sql = """
            INSERT INTO medallion.onepa_events 
            (event_id, title, url, outlet, start_date, session_time, registration_open, min_price, max_price, source)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        
        records = [tuple(x) for x in df.to_numpy()]
        
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.executemany(insert_sql, records)
        conn.commit()
        
        print(f"Successfully loaded {len(records)} OnePA events into medallion.onepa_events")

    load_to_gold()

dag3 = onepa_gold_load_pipeline()
