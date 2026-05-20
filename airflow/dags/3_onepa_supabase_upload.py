from datetime import datetime
import logging
from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define Dataset for scheduling
onepa_marts_dataset = Dataset("postgres://onepa_events.marts_onepa_events")

@dag(
    dag_id='3_onepa_supabase_upload',
    schedule=[onepa_marts_dataset],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['medallion', 'supabase', 'onepa', 'load'],
    description='Merges OnePA transformed data with Telegram images and uploads to Supabase'
)
def onepa_supabase_upload_pipeline():

    @task()
    def process_and_upload():
        # 1. Import helper modules inside task to prevent Airflow DAG parser from failing if dependencies are missing during webserver startup
        from telegram_scraper import scrape_telegram_channel, merge_and_download_images
        from supabase_exporter import export_to_supabase
        
        # 2. Extract transformed events from the local Postgres DB
        logger.info("Extracting transformed events from local Postgres DB...")
        try:
            hook = PostgresHook(postgres_conn_id='postgres_default')
            conn = hook.get_conn()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT event_id, title, url, outlet, start_date, session_time, registration_open, min_price, max_price, source
                FROM onepa_events.marts_onepa_events;
            """)
            columns = [desc[0] for desc in cursor.description]
            events = [dict(zip(columns, row)) for row in cursor.fetchall()]
            logger.info(f"Retrieved {len(events)} events from local Postgres.")
        except Exception as e:
            logger.error(f"Error querying local PostgreSQL database: {e}")
            raise Exception("Core database extraction failed. Pipeline aborted.")

        if not events:
            logger.warning("No events found in local Postgres. Nothing to process.")
            return

        # 3. Scrape Telegram public channel (robustly wrapped in try-except)
        telegram_messages = []
        try:
            telegram_messages = scrape_telegram_channel("othcommunity")
        except Exception as e:
            logger.error(f"Critical error during Telegram scraping: {e}. Proceeding with empty Telegram messages.")

        # 4. Merge Telegram messages/images and download matching images (robustly wrapped in try-except)
        merged_data = []
        try:
            logger.info("Running matching logic and downloading matching images...")
            merged_data = merge_and_download_images(events, telegram_messages)
            logger.info(f"Successfully matched and merged {len(merged_data)} events.")
        except Exception as e:
            logger.error(f"Critical error during merging or image download: {e}. Falling back to raw OnePA events.")
            # Fallback: Populate missing columns with None to preserve schema
            for event in events:
                event_copy = dict(event)
                event_copy['telegram_image_url'] = None
                event_copy['telegram_image_local_path'] = None
                event_copy['telegram_message_text'] = None
                merged_data.append(event_copy)

        # 5. Export final merged dataset to Supabase
        logger.info("Uploading final merged dataset to Supabase table 'onepa_csv'...")
        try:
            success = export_to_supabase(merged_data)
            if success:
                logger.info("Successfully exported merged dataset to Supabase!")
            else:
                logger.warning("Supabase export reported failure or upserted 0 rows. Check Supabase connection.")
        except Exception as e:
            logger.error(f"Critical error during Supabase upload: {e}")
            # We log the error but don't fail the task if we want it to show success,
            # or we can raise it if Supabase upload failure is a critical failure.
            # In a production pipeline, uploading to target destination is critical, so we raise.
            raise Exception(f"Supabase upload failed: {e}")

    process_and_upload()

dag3 = onepa_supabase_upload_pipeline()
