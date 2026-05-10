from datetime import datetime
import os
import requests
import time
from openpyxl import Workbook
# pyrefly: ignore [missing-import]
from airflow.decorators import dag, task
from airflow import Dataset

# Dataset definition for triggering downstream DAGs
onepa_bronze_dataset = Dataset("file:///opt/airflow/data/transform/onepa_bronze.xlsx")

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
    def scrape_onepa_to_bronze():
        # Setup session
        session = requests.Session()
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.onepa.gov.sg/",
            "Origin": "https://www.onepa.gov.sg"
        }
        session.headers.update(headers)

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
        
        # Save output into the Airflow container's data folder
        OUTPUT_FOLDER = "/opt/airflow/data/transform"
        if not os.path.exists(OUTPUT_FOLDER):
            os.makedirs(OUTPUT_FOLDER)

        wb = Workbook()
        ws = wb.active
        # Updated Headers to include all requested fields
        ws.append([
            "Event ID", "Title", "URL", "Outlet", "Start Date", 
            "Session Time", "Registration Open", "Min Price", "Max Price", "Source"
        ])

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
                            
                            ws.append([
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
                            ])

                    page += 1
                    time.sleep(0.3)
                except Exception as e:
                    print(f"Error skipping page: {e}")
                    break

        # Execute Master Scrape
        for period in TIME_PERIODS:
            print(f"\n=== PERIOD: {period.upper()} ===")
            for cat in CATEGORIES:
                scrape_data(cat, is_category=True, period=period)
            for kw in KEYWORDS:
                scrape_data(kw, is_category=False, period=period)

        file_path = os.path.join(OUTPUT_FOLDER, "onepa_bronze.xlsx")
        wb.save(file_path)
        print(f"\nExtraction Complete! Total Unique Events saved to {file_path}: {len(seen_ids)}")

    scrape_onepa_to_bronze()

dag1 = onepa_bronze_extract_pipeline()
