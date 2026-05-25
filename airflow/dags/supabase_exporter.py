import os
import re
import logging
import base64
import mimetypes
import json
from datetime import datetime, timezone
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

def file_to_base64(filepath):
    """
    Reads a file, converts it to base64, and returns the formatted data URL string.
    e.g. data:image/jpeg;base64,...
    """
    if not filepath or not os.path.exists(filepath):
        return None
        
    try:
        mime_type, _ = mimetypes.guess_type(filepath)
        if not mime_type:
            # Fallback to jpeg if mimetype cannot be guessed
            mime_type = "image/jpeg"
            
        with open(filepath, "rb") as image_file:
            encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
            return f"data:{mime_type};base64,{encoded_string}"
    except Exception as e:
        logger.error(f"Error encoding file {filepath} to base64: {e}")
        return None

_mappings_cache = None

def find_local_images(event_id, image_dir="/opt/airflow/data/telegram_images"):
    """
    Searches the image directory for any files matching the event_id,
    e.g. {event_id}.jpg, {event_id}_0.jpg, {event_id}.png, etc.
    Resolves duplicates using image_mappings.json if no files are directly found.
    Caches parsed mappings in memory to avoid repeated filesystem reads.
    """
    global _mappings_cache
    if not event_id or not os.path.exists(image_dir):
        return []
    
    event_id = str(event_id)
    matched_paths = []
    try:
        # List files in directory
        for filename in os.listdir(image_dir):
            # Check if file name matches {event_id}.ext or starts with {event_id}_
            name_without_ext, _ = os.path.splitext(filename)
            if name_without_ext == event_id or name_without_ext.startswith(f"{event_id}_"):
                matched_paths.append(os.path.join(image_dir, filename))
                
        # If no files found directly, check image_mappings.json for redirects
        if not matched_paths:
            if _mappings_cache is None:
                mapping_path = os.path.join(image_dir, "image_mappings.json")
                if os.path.exists(mapping_path):
                    with open(mapping_path, "r") as f:
                        _mappings_cache = json.load(f)
                else:
                    _mappings_cache = {}
            
            # k is the deleted duplicate filename, v is the kept unique filename
            for k, v in _mappings_cache.items():
                k_name, _ = os.path.splitext(k)
                if k_name == event_id or k_name.startswith(f"{event_id}_"):
                    target_path = os.path.join(image_dir, v)
                    if os.path.exists(target_path) and target_path not in matched_paths:
                        matched_paths.append(target_path)
    except Exception as e:
        logger.error(f"Error searching for local images in {image_dir}: {e}")
        
    # Sort paths to keep index order consistent (e.g. event_0.jpg before event_1.jpg)
    matched_paths.sort()
    return matched_paths

def clean_record_for_supabase(record):
    """
    Maps the source columns from the local table `marts_onepa_events` to the Supabase schema layout.
    """
    clean = {}
    
    # 1. Direct Named Maps
    clean['event_id'] = str(record.get('event_id', ''))
    clean['event_name'] = record.get('title') or record.get('event_name')
    clean['organiser_profile_name'] = record.get('outlet') or record.get('organiser_profile_name')
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
        clean['min_price'] = float(record.get('min_price', 0.0)) if record.get('min_price') is not None else 0.0
    except (ValueError, TypeError):
        clean['min_price'] = 0.0
        
    try:
        clean['max_price'] = float(record.get('max_price', 0.0)) if record.get('max_price') is not None else 0.0
    except (ValueError, TypeError):
        clean['max_price'] = 0.0
        
    clean['source'] = record.get('source')
    
    # 2. String Splitting Logic for session_time
    session_time = record.get('session_time')
    start_time, end_time = parse_session_time(session_time)
    clean['start_time'] = start_time or record.get('start_time')
    clean['end_time'] = end_time or record.get('end_time')
    
    # 3. Audit Timestamps (timezone-aware UTC datetime string)
    now_utc = datetime.now(timezone.utc).isoformat()
    clean['inserted_at'] = now_utc
    clean['updated_at'] = now_utc
    
    # 4. Image Mapping to array of Base64 text strings
    base64_images = []
    try:
        local_paths = record.get('telegram_image_local_paths', [])
        # Fallback to singular local path if present
        if not local_paths and record.get('telegram_image_local_path'):
            local_paths = [record.get('telegram_image_local_path')]
            
        # Fallback to checking local image directory
        if not local_paths:
            local_paths = find_local_images(clean.get('event_id'))
            
        if local_paths:
            for path in local_paths:
                b64_str = file_to_base64(path)
                if b64_str:
                    base64_images.append(b64_str)
    except Exception as e:
        logger.error(f"Error processing images to base64 for event {clean.get('event_id')}: {e}")
        base64_images = []
        
    # Check if there are base64 images already passed in the record (from live listener or offline scraped records)
    record_images = record.get('image_base64', [])
    if isinstance(record_images, str):
        record_images = [record_images]
    for b64 in record_images:
        if b64 and b64 not in base64_images:
            base64_images.append(b64)
            
    if base64_images:
        clean['image_base64'] = base64_images
    
    # 5. Populate from incoming record (preserving columns) or default to NULL (None in Python)
    clean['organiser_name'] = record.get('organiser_name')
    clean['description'] = record.get('description')
    clean['physical_venue'] = record.get('physical_venue')
    clean['physical_address'] = record.get('physical_address') or record.get('physical_venue')
    clean['event_start'] = record.get('event_start')
    clean['event_end'] = record.get('event_end')
    clean['status'] = record.get('status')
    clean['pa_event_type'] = record.get('pa_event_type')
    clean['end_date'] = record.get('end_date')
    
    return clean

def chunk_by_payload_size(records, max_bytes=2 * 1024 * 1024, max_count=50):
    """
    Chunks a list of records so that each chunk's JSON representation is under max_bytes,
    and has at most max_count records.
    """
    chunks = []
    current_chunk = []
    current_size = 0
    
    for record in records:
        record_size = len(json.dumps(record))
        if record_size > max_bytes:
            # If a single record is larger than max_bytes, it must be sent on its own
            if current_chunk:
                chunks.append(current_chunk)
                current_chunk = []
                current_size = 0
            chunks.append([record])
            continue
            
        if current_size + record_size > max_bytes or len(current_chunk) >= max_count:
            chunks.append(current_chunk)
            current_chunk = [record]
            current_size = record_size
        else:
            current_chunk.append(record)
            current_size += record_size
            
    if current_chunk:
        chunks.append(current_chunk)
        
    return chunks

def export_to_supabase(data):
    """
    Inserts/upserts the list of merged records to the Supabase table 'onepa_csv' in batches.
    Utilizes dynamic chunking to prevent hitting payload/timeout limits.
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
    
    # Decide chunking size based on if images are present.
    # If any record has images, use a smaller max count per batch to prevent long transactions.
    has_images = any(len(r.get('image_base64', [])) > 0 for r in cleaned_data)
    max_count = 10 if has_images else 50
    
    chunks = chunk_by_payload_size(cleaned_data, max_bytes=2 * 1024 * 1024, max_count=max_count)
    logger.info(f"Dynamically split {len(cleaned_data)} records into {len(chunks)} batches.")
    
    success_count = 0
    
    for idx, batch in enumerate(chunks):
        try:
            logger.info(f"Upserting batch {idx + 1}/{len(chunks)} ({len(batch)} records, estimated size: {len(json.dumps(batch))/1024:.1f} KB)...")
            
            response = client.table("onepa_csv").upsert(batch).execute()
            
            if response.data:
                success_count += len(response.data)
            else:
                success_count += len(batch)
                
        except Exception as e:
            logger.error(f"Error inserting batch {idx + 1}: {e}")
            logger.info("Attempting single-row insertions as fallback for this batch...")
            
            # If batch insert fails (e.g. duplicate key or row error), try inserting row-by-row
            # to maximize data load and isolate problem records.
            for record in batch:
                try:
                    client.table("onepa_csv").upsert(record).execute()
                    success_count += 1
                except Exception as row_error:
                    logger.error(f"Failed to load record {record.get('event_id')}: {row_error}")
            
    logger.info(f"Supabase export completed. Inserted/upserted approximately {success_count} records.")
    return success_count > 0
