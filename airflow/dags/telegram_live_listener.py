#!/usr/bin/env python3
import os
import re
import io
import sys
import logging
import asyncio
import base64
import mimetypes
from datetime import datetime, timezone

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from supabase import create_client, Client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("telegram_live_listener")

# List of channels to monitor
CHANNELS = [
    "othcommunity",
    "TNEvents",
    "tampgreenridges",
    "othsports",
    "otharts",
    "othlifestyle",
    "tampcentral",
    "tampeastcc",
    "tampinescentralcsc",
    "tampinesvista"
]

def clean_text(text):
    if not text:
        return ""
    return text.strip()

def parse_event_name(text):
    lines = [l.strip() for l in text.split('\n') if l.strip()]
    if not lines:
        return "Untitled Telegram Event"
    # Take the first line and strip markdown/headings
    title = lines[0]
    title = re.sub(r'[*_`~#|\[\]()]', '', title)
    if len(title) > 100:
        title = title[:97] + "..."
    return title.strip()

def parse_organiser(text, default="Our Tampines Hub"):
    org_match = re.search(r'(?:Organiser|Organised by|Organizer|Organized by):\s*(.*)', text, re.IGNORECASE)
    if org_match:
        # Strip trailing formatting
        val = org_match.group(1).strip()
        return re.sub(r'[*_`~#|]', '', val).strip()
    return default

def parse_venue(text, default="Our Tampines Hub"):
    venue_match = re.search(r'(?:Venue|Location|Where):\s*(.*)', text, re.IGNORECASE)
    if venue_match:
        val = venue_match.group(1).strip()
        return re.sub(r'[*_`~#|]', '', val).strip()
    return default

def parse_date_range(text):
    # Search for Date: line
    date_match = re.search(r'(?:Date|Dates):\s*(.*)', text, re.IGNORECASE)
    date_str = ""
    if date_match:
        date_str = date_match.group(1).strip()
    else:
        # Try to find a date pattern in the text, e.g. 24 May 2026
        found_dates = re.findall(r'\b\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4}\b', text, re.IGNORECASE)
        if found_dates:
            date_str = found_dates[0]
            
    if not date_str:
        return None, None
        
    # Clean string from markdown
    date_str = re.sub(r'[*_`~#|]', '', date_str).strip()
    
    # Split range
    parts = re.split(r'\s*[-–to]\s*', date_str, flags=re.IGNORECASE)
    start_date = parts[0].strip()
    end_date = parts[1].strip() if len(parts) > 1 else start_date
    return start_date, end_date

def parse_time_range(text):
    # Search for Time: line
    time_match = re.search(r'(?:Time|Session Time):\s*(.*)', text, re.IGNORECASE)
    time_str = ""
    if time_match:
        time_str = time_match.group(1).strip()
    else:
        # Try to find a time pattern (e.g. 2:00 PM - 5:00 PM)
        found_times = re.findall(r'\b\d{1,2}(?::\d{2})?\s*(?:AM|PM|am|pm)?\s*[-–to]\s*\d{1,2}(?::\d{2})?\s*(?:AM|PM|am|pm)\b', text, re.IGNORECASE)
        if found_times:
            time_str = found_times[0]
            
    if not time_str:
        return None, None
        
    time_str = re.sub(r'[*_`~#|]', '', time_str).strip()
    parts = re.split(r'\s*[-–to]\s*', time_str, flags=re.IGNORECASE)
    start_time = parts[0].strip()
    end_time = parts[1].strip() if len(parts) > 1 else None
    return start_time, end_time

def parse_prices(text):
    text_lower = text.lower()
    if "free" in text_lower:
        return 0.0, 0.0
        
    # Find all $ patterns
    prices = [float(p) for p in re.findall(r'\$\s*(\d+(?:\.\d{2})?)', text)]
    if prices:
        return min(prices), max(prices)
    return 0.0, 0.0

def parse_status_and_registration(text):
    text_lower = text.lower()
    if any(k in text_lower for k in ["fully booked", "closed", "registration closed", "cancelled"]):
        return "Closed", False
    return "Active", True

def message_to_record(text, msg_id, channel_name, base64_images=None):
    text = text or ""
    
    # 1. Parsing target columns
    event_id = f"tele_live_{channel_name}_{msg_id}"
    event_name = parse_event_name(text)
    organiser = parse_organiser(text)
    venue = parse_venue(text)
    start_date, end_date = parse_date_range(text)
    start_time, end_time = parse_time_range(text)
    min_price, max_price = parse_prices(text)
    status, reg_open = parse_status_and_registration(text)
    
    # Extract URL
    url_match = re.search(r'https?://[^\s]+', text)
    url = url_match.group(0) if url_match else f"https://t.me/{channel_name}/{msg_id}"
    
    now_utc = datetime.now(timezone.utc).isoformat()
    
    record = {
        'event_id': event_id,
        'organiser_name': organiser,
        'organiser_profile_name': channel_name,
        'event_name': event_name,
        'description': text,
        'physical_venue': venue,
        'physical_address': venue,  # Default address to venue
        'event_start': None,        # Can be set if full ISO timestamp is parsed
        'event_end': None,
        'status': status,
        'pa_event_type': "Interest Group",
        'end_date': end_date,
        'end_time': end_time,
        'start_date': start_date,
        'start_time': start_time,
        'url': url,
        'registration_open': reg_open,
        'min_price': min_price,
        'max_price': max_price,
        'source': "telegram_live_oth",
        'inserted_at': now_utc,
        'updated_at': now_utc,
    }
    
    if base64_images:
        record['image_base64'] = base64_images
        
    return record

def get_supabase_client():
    supabase_url = os.environ.get("SUPABASE_URL") or os.environ.get("SUPABASE_DATABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    
    if not supabase_url or "your-project" in supabase_url:
        logger.error("SUPABASE_URL is not configured.")
        return None
    if not supabase_key or "your-supabase" in supabase_key:
        logger.error("SUPABASE_KEY is not configured.")
        return None
        
    cleaned_url = supabase_url.strip()
    cleaned_url = re.sub(r'/rest/v1/?$', '', cleaned_url)
    
    try:
        return create_client(cleaned_url, supabase_key)
    except Exception as e:
        logger.error(f"Failed to create Supabase client: {e}")
        return None

async def upload_to_supabase(record):
    client = get_supabase_client()
    if not client:
        logger.warning("Supabase client not initialized. Skipping upload.")
        return
        
    try:
        logger.info(f"Upserting event {record.get('event_id')} ('{record.get('event_name')}') to Supabase...")
        # Run sync Supabase call in a separate thread to prevent blocking Telethon event loop
        response = await asyncio.to_thread(client.table("onepa_csv").upsert(record).execute)
        if response.data:
            logger.info(f"Successfully upserted event {record.get('event_id')} to Supabase.")
        else:
            logger.warning(f"Upsert returned empty data for event {record.get('event_id')}.")
    except Exception as e:
        logger.error(f"Error upserting event {record.get('event_id')} to Supabase: {e}")

async def download_message_photo_base64(client, message):
    if not message.photo:
        return None
    try:
        buffer = io.BytesIO()
        await client.download_media(message.photo, file=buffer)
        buffer.seek(0)
        
        # Convert buffer to base64
        encoded = base64.b64encode(buffer.read()).decode('utf-8')
        return f"data:image/jpeg;base64,{encoded}"
    except Exception as e:
        logger.error(f"Error downloading photo in memory: {e}")
        return None

def run_dry_run_test():
    print("=== Running Parser Test Simulation ===")
    test_text = """
**OTH Community Line Dance Fiesta**
Organised by: Tampines Central Sports Network
Venue: Festive Plaza, Our Tampines Hub
Address: 1 Tampines Walk, Singapore 528523
Date: 24 May 2026 - 25 May 2026
Time: 6:00 PM - 9:00 PM
Price: $5.00
Registration: Open at https://go.gov.sg/linedance2026
Status: Open
Description: Join us for an evening of line dancing fun!
"""
    record = message_to_record(test_text, 99999, "othcommunity", ["data:image/jpeg;base64,TEST_DATA"])
    print("\nParsed Record:")
    for k, v in record.items():
        if k == 'image_base64':
            print(f"{k}: [Base64 string of length {len(v[0])}]")
        elif k == 'description':
            print(f"{k}: [Text of length {len(v)}]")
        else:
            print(f"{k}: {v}")
    
    # Assertions
    assert record['event_name'] == "OTH Community Line Dance Fiesta", "Event name parsing failed"
    assert record['organiser_name'] == "Tampines Central Sports Network", "Organiser parsing failed"
    assert record['physical_venue'] == "Festive Plaza, Our Tampines Hub", "Venue parsing failed"
    assert record['start_date'] == "24 May 2026", "Start date parsing failed"
    assert record['end_date'] == "25 May 2026", "End date parsing failed"
    assert record['start_time'] == "6:00 PM", "Start time parsing failed"
    assert record['end_time'] == "9:00 PM", "End time parsing failed"
    assert record['min_price'] == 5.0, "Min price parsing failed"
    assert record['max_price'] == 5.0, "Max price parsing failed"
    assert record['registration_open'] is True, "Registration open parsing failed"
    assert record['url'] == "https://go.gov.sg/linedance2026", "URL parsing failed"
    print("\nAll parser tests PASSED successfully!")

async def start_listener():
    api_id = os.environ.get("TELEGRAM_API_ID")
    api_hash = os.environ.get("TELEGRAM_API_HASH")
    session_str = os.environ.get("TELEGRAM_STRING_SESSION")
    
    if not api_id or not api_hash or not session_str:
        logger.error("Missing TELEGRAM_API_ID, TELEGRAM_API_HASH, or TELEGRAM_STRING_SESSION env variables.")
        logger.error("Please run scripts/telegram_login.py on your host to authorize and obtain a session string.")
        sys.exit(1)
        
    logger.info("Initializing Telethon Live Listener client...")
    client = TelegramClient(StringSession(session_str), int(api_id), api_hash)
    
    # 1. Album (multi-image) handler
    @client.on(events.Album(chats=CHANNELS))
    async def handler_album(event):
        try:
            channel_name = event.chat.username or str(event.chat_id)
            msg_id = event.messages[0].id
            text = event.text
            
            logger.info(f"Captured live ALBUM update from '{channel_name}', ID {msg_id}")
            
            # Download all photos in the album
            base64_images = []
            for msg in event.messages:
                if msg.photo:
                    b64 = await download_message_photo_base64(client, msg)
                    if b64:
                        base64_images.append(b64)
                        
            record = message_to_record(text, msg_id, channel_name, base64_images)
            await upload_to_supabase(record)
        except Exception as err:
            logger.error(f"Error handling album update: {err}")

    # 2. New Message (single image or text-only) handler
    @client.on(events.NewMessage(chats=CHANNELS))
    async def handler_new_message(event):
        # Ignore if it is part of an album (grouped_id is set) to prevent double execution
        if event.grouped_id:
            return
            
        try:
            channel_name = event.chat.username or str(event.chat_id)
            msg_id = event.id
            text = event.text
            
            logger.info(f"Captured live MESSAGE update from '{channel_name}', ID {msg_id}")
            
            base64_images = []
            if event.photo:
                b64 = await download_message_photo_base64(client, event.message)
                if b64:
                    base64_images.append(b64)
                    
            record = message_to_record(text, msg_id, channel_name, base64_images)
            await upload_to_supabase(record)
        except Exception as err:
            logger.error(f"Error handling single message update: {err}")
            
    logger.info("Connecting to Telegram...")
    await client.start()
    logger.info("Successfully connected. Listening for updates in real time...")
    await client.run_until_disconnected()

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        run_dry_run_test()
    else:
        asyncio.run(start_listener())
