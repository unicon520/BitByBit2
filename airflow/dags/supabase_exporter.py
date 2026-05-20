import os
import re
import logging
from supabase import create_client, Client

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_supabase_client():
    """
    Initializes and returns the Supabase client using environment variables.
    """
    supabase_url = os.environ.get("SUPABASE_URL") or os.environ.get("SUPABASE_DATABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    
    if not supabase_url or "your-project" in supabase_url:
        logger.warning("SUPABASE_URL / SUPABASE_DATABASE_URL is not configured or is using the default placeholder.")
        return None
    if not supabase_key or "your-supabase" in supabase_key:
        logger.warning("SUPABASE_KEY is not configured or is using the default placeholder.")
        return None
        
    # Strip any trailing /rest/v1/ REST API endpoints if pasted in URL
    cleaned_url = supabase_url.strip()
    cleaned_url = re.sub(r'/rest/v1/?$', '', cleaned_url)
    
    try:
        client: Client = create_client(cleaned_url, supabase_key)
        return client
    except Exception as e:
        logger.error(f"Failed to create Supabase client with URL '{cleaned_url}': {e}")
        return None


def parse_session_time(session_time):
    """
    Robust parsing function to extract start_time and end_time from a session_time string.
    Handles intervals like "HH:MM - HH:MM", "HH:MM AM - HH:MM PM", "HH.MM AM to HH.MM PM", etc.
    """
    if not session_time or not isinstance(session_time, str):
        return None, None
        
    session_time = session_time.strip()
    if not session_time:
        return None, None
        
    try:
        # Standardize separators: replace "to" or "and" with "-"
        standardized = re.sub(r'\s+to\s+|\s+and\s+', ' - ', session_time, flags=re.IGNORECASE)
        
        # Split by dash
        if '-' in standardized:
            parts = standardized.split('-')
            if len(parts) == 2:
                return parts[0].strip(), parts[1].strip()
                
        # Regex fallback to find two time patterns
        # e.g., "10:00 AM", "14:00", "2.00 PM"
        time_pattern = r'\d{1,2}[:.]\d{2}(?:\s*(?:AM|PM))?'
        matches = re.findall(time_pattern, session_time, re.IGNORECASE)
        if len(matches) >= 2:
            return matches[0].strip(), matches[1].strip()
        elif len(matches) == 1:
            return matches[0].strip(), None
            
    except Exception as e:
        logger.warning(f"Error parsing session_time '{session_time}': {e}. Returning None.")
        
    return None, None

def clean_record_for_supabase(record):
    """
    Maps the source columns from the local table `marts_onepa_events` to the Supabase schema layout.
    """
    clean = {}
    
    # 1. Direct Named Maps
    clean['event_id'] = str(record.get('event_id', ''))
    clean['event_name'] = record.get('title')
    clean['organiser_profile_name'] = record.get('outlet')
    clean['start_date'] = record.get('start_date')
    clean['url'] = record.get('url')
    
    # Cast boolean safely
    reg_open = record.get('registration_open')
    if isinstance(reg_open, str):
        clean['registration_open'] = reg_open.lower() in ('true', '1', 'yes')
    else:
        clean['registration_open'] = bool(reg_open) if reg_open is not None else False
        
    # Cast numeric prices safely
    try:
        clean['min_price'] = float(record.get('min_price', 0.0))
    except (ValueError, TypeError):
        clean['min_price'] = 0.0
        
    try:
        clean['max_price'] = float(record.get('max_price', 0.0))
    except (ValueError, TypeError):
        clean['max_price'] = 0.0
        
    clean['source'] = record.get('source')
    
    # 2. String Splitting Logic for session_time
    session_time = record.get('session_time')
    start_time, end_time = parse_session_time(session_time)
    clean['start_time'] = start_time
    clean['end_time'] = end_time
    
    # 3. Unmapped Columns set to NULL (None in Python)
    clean['organiser_name'] = None
    clean['description'] = None
    clean['physical_venue'] = None
    clean['physical_address'] = None
    clean['event_start'] = None
    clean['event_end'] = None
    clean['status'] = None
    clean['pa_event_type'] = None
    clean['end_date'] = None
    
    return clean

def export_to_supabase(data, batch_size=100):
    """
    Inserts/upserts the list of merged records to the Supabase table 'onepa_csv' in batches.
    """
    client = get_supabase_client()
    if not client:
        logger.error("Supabase client is not available. Skipping export to Supabase.")
        return False
        
    cleaned_data = [clean_record_for_supabase(row) for row in data if row.get('event_id')]
    
    if not cleaned_data:
        logger.warning("No valid records to upload to Supabase.")
        return False
        
    logger.info(f"Preparing to insert {len(cleaned_data)} records into Supabase table 'onepa_csv'...")
    
    success_count = 0
    
    for i in range(0, len(cleaned_data), batch_size):
        batch = cleaned_data[i:i + batch_size]
        try:
            logger.info(f"Upserting batch {i//batch_size + 1} ({len(batch)} records)...")
            
            # Use supabase.table().upsert() to automatically resolve duplicate event_id conflicts
            response = client.table("onepa_csv").upsert(batch).execute()
            
            if response.data:
                success_count += len(response.data)
            else:
                success_count += len(batch)

                
        except Exception as e:
            logger.error(f"Error inserting batch {i//batch_size + 1}: {e}")
            logger.info("Attempting single-row insertions as fallback for this batch...")
            
            # If batch insert fails (e.g. duplicate key or row error), try inserting row-by-row
            # to maximize data load and isolate problem records.
            for record in batch:
                try:
                    # We can use upsert or catch insert errors
                    client.table("onepa_csv").upsert(record).execute()
                    success_count += 1
                except Exception as row_error:
                    logger.error(f"Failed to load record {record.get('event_id')}: {row_error}")
            
    logger.info(f"Supabase export completed. Inserted/upserted approximately {success_count} records.")
    return success_count > 0
