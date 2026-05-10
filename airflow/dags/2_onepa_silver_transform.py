from datetime import datetime
import pandas as pd
# pyrefly: ignore [missing-import]
from airflow.decorators import dag, task
# pyrefly: ignore [missing-import]
from airflow import Dataset

# Dataset definitions
onepa_bronze_dataset = Dataset("file:///opt/airflow/data/transform/onepa_bronze.xlsx")
onepa_silver_dataset = Dataset("file:///opt/airflow/data/load/onepa_silver.csv")

@dag(
    dag_id='2_onepa_silver_transform',
    schedule=[onepa_bronze_dataset], # Automatically runs when Bronze completes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['medallion', 'silver', 'onepa', 'transform'],
    description='Cleans Bronze OnePA data and saves as Silver'
)
def onepa_silver_transform_pipeline():

    @task(outlets=[onepa_silver_dataset])
    def transform_to_silver():
        bronze_file_path = '/opt/airflow/data/transform/onepa_bronze.xlsx'
        silver_file_path = '/opt/airflow/data/load/onepa_silver.csv'
        
        print(f"Reading Bronze data from {bronze_file_path}")
        # Need openpyxl engine to read xlsx
        df = pd.read_excel(bronze_file_path, engine='openpyxl')
        
        # Silver Transformations:
        # 1. Standardize column names
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
        
        # 2. Handle missing prices
        df['min_price'] = df['min_price'].fillna(0.0)
        df['max_price'] = df['max_price'].fillna(0.0)
        
        # 3. Ensure event_id is a string and handle missing titles
        df['event_id'] = df['event_id'].astype(str)
        df = df.dropna(subset=['title'])
        
        # Convert boolean registration_open to proper string/bool format
        df['registration_open'] = df['registration_open'].astype(bool)

        # Save to silver (CSV format is usually better for fast DB loading)
        df.to_csv(silver_file_path, index=False)
        print(f"Saved Silver data to {silver_file_path} with {len(df)} rows.")

    transform_to_silver()

dag2 = onepa_silver_transform_pipeline()
