from datetime import datetime
import os
import requests
import time
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.datasets import Dataset

# Dataset definition for triggering downstream DAGs
onepa_bronze_dataset = Dataset("postgres://raw.onepa_bronze")

@dag(
    dag_id='1_onepa_bronze_extract',
    schedule_interval='@weekly', # Can be adjusted as needed
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['medallion', 'bronze', 'onepa', 'extract'],
    description='Extracts event data from OnePA website and saves as Bronze'
)
def onepa_bronze_extract_pipeline():

    @task(outlets=[onepa_bronze_dataset])
    def extract_and_load_raw():
        # Setup session
        session = requests.Session()
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.onepa.gov.sg/",
            "Origin": "https://www.onepa.gov.sg",
            "X-Requested-With": "XMLHttpRequest"
        }
        session.headers.update(headers)

        # Make an initial request to the homepage to get any necessary cookies (like session/WAF cookies)
        try:
            print("Establishing session by visiting the homepage...")
            session.get("https://www.onepa.gov.sg/", timeout=15)
            time.sleep(2)
        except Exception as e:
            print(f"Warning: Could not load homepage: {e}")

        CATEGORIES = [ 
            "Active Ageing", "Arts & Culture", "Celebration & Festivity", "Charity & Volunteerism", 
            "Chinese New Year", "Christmas", "Competitions", "Deepavali", "Exhibition & Fair", 
            "Hari Raya Haji", "Health & Fitness", "Kopi Talks & Dialogues", "Local Outings & Tours", 
            "National Day", "Neighbourhood Events", "Overseas Outings & Tours", "Parenting & Education" 
        ]

        KEYWORDS = [
            "ageing", "arts", "culture", "fitness", "health",
            "dance", "education", "parenting", "festival",
            "volunteer", "competition", "tour"
        ]

        TIME_PERIODS = ["this-month", "next-month"]
        BASE_URL = "https://www.onepa.gov.sg/pacesapi/eventsearch/searchjson"
        
        seen_ids = set()
        events_data = []

        seen_ids = set()

        def scrape_data(search_val, is_category=True, period="this-month"):
            page = 1
            while True:
                params = {
                    "events": "" if is_category else search_val,
                    "aoi": search_val if is_category else "",
                    "outlet": "",
                    "timePeriod": period,
                    "sort": "rel",
                    "page": page
                }

                try:
                    res = session.get(BASE_URL, params=params, timeout=15)
                    res.raise_for_status()
                    data = res.json()
                    items = data.get("data", {}).get("results", [])

                    if not items:
                        break
                    
                    print(f"  - {search_val} ({period}) Page {page}: {len(items)} items")

                    for item in items:
                        event_id = item.get("eventId")
                        if event_id and event_id not in seen_ids:
                            seen_ids.add(event_id)
                            
                            title = item.get("title")
                            share_url = item.get("share", {}).get("url")
                            product_url = item.get("productUrl")
                            final_url = share_url if share_url else f"https://www.onepa.gov.sg{product_url}"
                            
                            events_data.append((
                                event_id,
                                title,
                                final_url,
                                item.get("outlet"),
                                item.get("startDate"),
                                item.get("sessionTime"),
                                item.get("isRegistrationOpen"),
                                item.get("minPrice"),
                                item.get("maxPrice"),
                                f"{search_val} ({period})"
                            ))

                    page += 1
                    time.sleep(2)  # Increased sleep time to prevent rate limiting (403 Forbidden)
                except Exception as e:
                    print(f"Error on page {page}: {e}")
                    break

        # Execute Master Scrape
        for period in TIME_PERIODS:
            print(f"\n=== PERIOD: {period.upper()} ===")
            for cat in CATEGORIES:
                scrape_data(cat, is_category=True, period=period)
                time.sleep(1)
            for kw in KEYWORDS:
                scrape_data(kw, is_category=False, period=period)
                time.sleep(1)

        if len(seen_ids) == 0:
            raise Exception("Failed to extract any data! All requests might have been blocked (403 Forbidden).")

        hook = PostgresHook(postgres_conn_id='postgres_default')
        setup_sql = """
            CREATE SCHEMA IF NOT EXISTS raw;
            CREATE TABLE IF NOT EXISTS raw.onepa_bronze (
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
            TRUNCATE TABLE raw.onepa_bronze;
        """
        hook.run(setup_sql)

        insert_sql = """
            INSERT INTO raw.onepa_bronze 
            (event_id, title, url, outlet, start_date, session_time, registration_open, min_price, max_price, source)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING;
        """
        
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.executemany(insert_sql, events_data)
        conn.commit()

        print(f"\nExtraction Complete! Total Unique Events saved to raw.onepa_bronze: {len(events_data)}")

    extract_and_load_raw()

dag1 = onepa_bronze_extract_pipeline()
