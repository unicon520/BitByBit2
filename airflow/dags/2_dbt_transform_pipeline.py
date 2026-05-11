import os
from datetime import datetime
from airflow import DAG
from airflow.datasets import Dataset
# pyrefly: ignore [missing-import]
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
# pyrefly: ignore [missing-import]
from cosmos.profiles import PostgresUserPasswordProfileMapping

# We listen for the completion of the bronze extraction
onepa_bronze_dataset = Dataset("file:///opt/airflow/data/transform/onepa_bronze.xlsx") # Or we could just use the DAG's regular schedule, but keeping dataset for compatibility. Actually we refactored extract to postgres. Let's update the dataset to a custom URI or keep as is.
# Let's change the dataset in extract and here to a database dataset, but for simplicity let's just trigger after dag 1, or use a dataset.

DBT_PROJECT_PATH = "/opt/airflow/dags/dbt_onepa"

profile_config = ProfileConfig(
    profile_name="onepa_elt",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_default",
        profile_args={"schema": "onepa_events"},
    )
)

with DAG(
    dag_id="2_dbt_transform_pipeline",
    schedule=[Dataset("postgres://raw.onepa_bronze")],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["medallion", "dbt", "transform", "vectorize"],
    description="Runs dbt transformations and vector embeddings using cosmos"
) as dag:

    transformations = DbtTaskGroup(
        group_id="dbt_transformations",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path="dbt"),
    )

    transformations
