import os
import re
import requests
import logging
import difflib
import json
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Directory inside the container where images will be saved
IMAGE_DIR = "/opt/airflow/data/telegram_images"

def clean_text(text):
    """Clean text by lowercasing and removing punctuation/special chars."""
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', ' ', text)
    return " ".join(text.split())

def calculate_fuzzy_match(title, text):
    """
    Performs an optimized sliding-window fuzzy match of the title inside a longer text.
    Returns the maximum similarity ratio found, or 1.0 if exact substring match.
    """
    if not title or not text:
        return 0.0
        
    c_title = clean_text(title)
    c_text = clean_text(text)
    
    if not c_title or not c_text:
        return 0.0
        
    # Check for exact substring match first (100% score)
    if c_title in c_text:
        return 1.0
        
    words_title = c_title.split()
    words_text = c_text.split()
    
    n_title = len(words_title)
    n_text = len(words_text)
    
    if n_text < n_title:
        # Title is longer than text, match global ratio
        return difflib.SequenceMatcher(None, c_title, c_text).ratio()
        
    # Fast pre-filter: check if at least one meaningful keyword from the title is present in the text
    stop_words = {'the', 'a', 'in', 'at', 'on', 'of', 'and', 'for', 'with', 'by', 'is', 'to', 'from', 'our'}
    title_keywords = {w for w in words_title if w not in stop_words and len(w) > 2}
    
    if title_keywords:
        text_joined = " " + c_text + " "
        if not any(kw in text_joined for kw in title_keywords):
            return 0.0
            
    # Sliding window fuzzy match
    best_ratio = 0.0
    for i in range(n_text - n_title + 1):
        sub_text = " ".join(words_text[i:i+n_title])
        ratio = difflib.SequenceMatcher(None, c_title, sub_text).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            if best_ratio >= 0.95:
                break
                
    return best_ratio

def match_event_to_telegram(event, telegram_messages):
    """
    Matches a single OnePA event to the best Telegram post based on title similarity.
    Considers the post a match if ratio >= 0.85.
    Returns the matched Telegram post or None.
    """
    event_id = str(event.get('event_id', ''))
    event_title = event.get('title', '')
    
    if not event_title:
        return None
        
    best_match = None
    best_score = 0.0
    
    for msg in telegram_messages:
        msg_text = msg.get("text", "")
        if not msg_text:
            continue
            
        # 1. Explicit ID Matching
        if event_id in msg_text or event_id in msg.get("post_id", ""):
            logger.info(f"Explicit ID Match found for Event {event_id} in Telegram post {msg.get('post_id')}")
            return msg
            
        # 2. Sliding Window Fuzzy Title Matching
        score = calculate_fuzzy_match(event_title, msg_text)
        if score >= 0.85 and score > best_score:
            best_score = score
            best_match = msg
            
    if best_match:
        logger.info(f"Fuzzy Match found for Event {event_id} ('{event_title}') with score {best_score:.2f} in Telegram post {best_match.get('post_id')}")
    return best_match

def get_unique_image_hashes():
    """
    Computes and returns a dictionary mapping MD5 hashes to filenames of unique images
    currently present in IMAGE_DIR. Excludes files mapped as duplicates in image_mappings.json.
    """
    import hashlib
    hash_map = {}
    if not os.path.exists(IMAGE_DIR):
        return hash_map
        
    # Read mappings to know which files to exclude
    mapping_path = os.path.join(IMAGE_DIR, "image_mappings.json")
    excluded = set()
    if os.path.exists(mapping_path):
        try:
            with open(mapping_path, "r") as f:
                mappings = json.load(f)
                excluded.update(mappings.keys())
        except Exception as e:
            logger.error(f"Error reading image_mappings.json: {e}")
            
    try:
        for filename in os.listdir(IMAGE_DIR):
            if filename == "image_mappings.json" or filename in excluded:
                continue
            filepath = os.path.join(IMAGE_DIR, filename)
            if os.path.isfile(filepath):
                # Calculate MD5
                hasher = hashlib.md5()
                with open(filepath, "rb") as f:
                    for chunk in iter(lambda: f.read(4096), b""):
                        hasher.update(chunk)
                h = hasher.hexdigest()
                hash_map[h] = filename
    except Exception as e:
        logger.error(f"Error building unique image hashes: {e}")
        
    return hash_map

def save_or_dedup_image(temp_path, target_filename, unique_hashes):
    """
    Checks if the image at temp_path is a duplicate.
    If it is, deletes temp_path and adds a redirect to image_mappings.json, returning the path of the existing unique image.
    If it is not, renames temp_path to target_filename, updates unique_hashes, and returns the path to target_filename.
    """
    import hashlib
    
    if not os.path.exists(temp_path):
        return None
        
    # Calculate MD5 of temp file
    hasher = hashlib.md5()
    with open(temp_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    h = hasher.hexdigest()
    
    mapping_path = os.path.join(IMAGE_DIR, "image_mappings.json")
    os.makedirs(IMAGE_DIR, exist_ok=True)
    
    # Load mappings
    mappings = {}
    if os.path.exists(mapping_path):
        try:
            with open(mapping_path, "r") as f:
                mappings = json.load(f)
        except Exception as e:
            logger.error(f"Error loading image_mappings.json: {e}")
            
    if h in unique_hashes:
        # It's a duplicate!
        existing_filename = unique_hashes[h]
        logger.info(f"Duplicate image content detected for {target_filename}. Mapping to {existing_filename}.")
        mappings[target_filename] = existing_filename
        try:
            with open(mapping_path, "w") as f:
                json.dump(mappings, f, indent=2)
            # Remove temp file if it's not the unique file itself
            if os.path.abspath(temp_path) != os.path.abspath(os.path.join(IMAGE_DIR, existing_filename)):
                os.remove(temp_path)
        except Exception as e:
            logger.error(f"Error during duplicate cleanup or mapping save: {e}")
        return os.path.join(IMAGE_DIR, existing_filename)
    else:
        # Unique image!
        final_path = os.path.join(IMAGE_DIR, target_filename)
        if os.path.abspath(temp_path) != os.path.abspath(final_path):
            try:
                if os.path.exists(final_path):
                    os.remove(final_path)
                os.rename(temp_path, final_path)
            except Exception as e:
                logger.error(f"Error moving temp file {temp_path} -> {final_path}: {e}")
                return temp_path
        unique_hashes[h] = target_filename
        if target_filename in mappings:
            del mappings[target_filename]
            try:
                with open(mapping_path, "w") as f:
                    json.dump(mappings, f, indent=2)
            except Exception as e:
                logger.error(f"Error updating image_mappings.json: {e}")
        return final_path

def download_images(image_urls, event_id):
    """
    Downloads list of image_urls and saves them to IMAGE_DIR.
    Dynamically deduplicates image content.
    Returns a list of local paths.
    """
    if not image_urls:
        return []
        
    local_paths = []
    os.makedirs(IMAGE_DIR, exist_ok=True)
    
    unique_hashes = get_unique_image_hashes()
    
    for idx, url in enumerate(image_urls):
        if not url:
            continue
        try:
            # Determine extension
            ext = ".jpg"
            if ".png" in url.lower():
                ext = ".png"
            elif ".webp" in url.lower():
                ext = ".webp"
                
            local_filename = f"{event_id}_{idx}{ext}"
            local_path = os.path.join(IMAGE_DIR, local_filename)
            
            # Check mapping first (if we have previously mapped this filename)
            mapping_path = os.path.join(IMAGE_DIR, "image_mappings.json")
            if os.path.exists(mapping_path):
                with open(mapping_path, "r") as f:
                    mappings = json.load(f)
                if local_filename in mappings:
                    mapped_target = os.path.join(IMAGE_DIR, mappings[local_filename])
                    if os.path.exists(mapped_target):
                        logger.info(f"Image {local_filename} already mapped to unique image {mappings[local_filename]}. Skipping download.")
                        local_paths.append(mapped_target)
                        continue
            
            # Check if already downloaded as a unique image
            if os.path.exists(local_path):
                logger.info(f"Image {local_filename} already exists. Skipping download.")
                local_paths.append(local_path)
                continue
                
            # Otherwise download to a temporary file in IMAGE_DIR
            temp_filename = f"temp_{event_id}_{idx}{ext}"
            temp_path = os.path.join(IMAGE_DIR, temp_filename)
            
            logger.info(f"Downloading image {idx} for event {event_id} from {url}...")
            response = requests.get(url, stream=True, timeout=15)
            response.raise_for_status()
            
            with open(temp_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    
            # Save or deduplicate
            final_path = save_or_dedup_image(temp_path, local_filename, unique_hashes)
            if final_path:
                logger.info(f"Successfully processed image to {final_path}")
                local_paths.append(final_path)
        except Exception as e:
            logger.error(f"Failed to download image {idx} for event {event_id} from {url}: {e}")
            if 'temp_path' in locals() and os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass
            
    return local_paths

async def scrape_telegram_telethon(api_id, api_hash, session_str, channel_username="othcommunity", limit=50):
    """
    Scrapes a public Telegram channel using Telethon API.
    Downloads the media associated with the posts to the local IMAGE_DIR.
    Returns a list of dicts: [{'post_id': str, 'text': str, 'image_paths': list, 'date': str}]
    """
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    
    logger.info(f"Connecting to Telegram via Telethon for channel: {channel_username}...")
    try:
        client = TelegramClient(StringSession(session_str), api_id, api_hash)
        await client.connect()
        
        if not await client.is_user_authorized():
            logger.error("Telethon client is not authorized. Please check your TELEGRAM_STRING_SESSION env variable.")
            await client.disconnect()
            return []
            
        logger.info("Telethon authorization successful. Fetching channel history...")
        
        raw_posts = []
        os.makedirs(IMAGE_DIR, exist_ok=True)
        
        async for message in client.iter_messages(channel_username, limit=limit):
            raw_posts.append(message)
            
        # Group messages by grouped_id (if part of an album) or message ID (if single)
        posts_by_group = {}
        for msg in raw_posts:
            g_id = msg.grouped_id if msg.grouped_id else f"single_{msg.id}"
            if g_id not in posts_by_group:
                posts_by_group[g_id] = {
                    'messages': [],
                    'caption': "",
                    'id': msg.id,
                    'date': msg.date
                }
            posts_by_group[g_id]['messages'].append(msg)
            if msg.text:
                # Use the longest text in the group as the main caption
                if len(msg.text) > len(posts_by_group[g_id]['caption']):
                    posts_by_group[g_id]['caption'] = msg.text

        posts = []
        for g_id, group in posts_by_group.items():
            post_id = f"{channel_username}/{group['id']}"
            text = group['caption']
            timestamp = group['date'].isoformat() if group['date'] else ""
            
            image_paths = []
            # Download photos for all messages in this group/album
            for idx, msg in enumerate(group['messages']):
                if msg.photo:
                    local_path = os.path.join(IMAGE_DIR, f"telethon_{msg.id}_{idx}.jpg")
                    try:
                        await client.download_media(msg.photo, file=local_path)
                        image_paths.append(local_path)
                    except Exception as media_err:
                        logger.error(f"Failed to download Telethon media for message {msg.id}: {media_err}")
                        
            posts.append({
                'post_id': post_id,
                'text': text,
                'image_paths': image_paths,
                'image_urls': [],
                'date': timestamp
            })
            
        await client.disconnect()
        logger.info(f"Telethon finished. Scraped {len(posts)} grouped posts.")
        return posts
    except Exception as e:
        logger.error(f"Error in Telethon scraper execution: {e}")
        return []

def scrape_telegram_bs4(channel_username="othcommunity"):
    """
    Scrapes the public preview of a Telegram channel using BeautifulSoup.
    Returns a list of dicts: [{'post_id': str, 'text': str, 'image_urls': list, 'date': str}]
    """
    url = f"https://t.me/s/{channel_username}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    try:
        logger.info(f"Fetching Telegram public preview page for {channel_username}...")
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Error fetching Telegram preview page: {e}. Skipping Telegram scraping.")
        return []
    
    soup = BeautifulSoup(response.content, 'html.parser')
    message_elements = soup.find_all('div', class_='tgme_widget_message')
    
    scraped_posts = []
    logger.info(f"Found {len(message_elements)} message containers in preview.")
    
    for msg in message_elements:
        post_id = msg.get('data-post', '')
        
        # Extract text
        text_elem = msg.find('div', class_='tgme_widget_message_text')
        text = text_elem.get_text(separator=" ").strip() if text_elem else ""
        
        # Extract timestamp
        time_elem = msg.find('time', class_='time')
        timestamp = time_elem.get('datetime', '') if time_elem else ""
        
        # Extract image URLs
        image_urls = []
        photo_elems = msg.find_all('a', class_='tgme_widget_message_photo_wrap')
        for photo_elem in photo_elems:
            if 'style' in photo_elem.attrs:
                style_content = photo_elem['style']
                match = re.search(r"url\(['\"]?(.*?)['\"]?\)", style_content)
                if match:
                    image_urls.append(match.group(1))
        
        if text or image_urls:
            scraped_posts.append({
                'post_id': post_id,
                'text': text,
                'image_urls': image_urls,
                'image_paths': [],
                'date': timestamp
            })
            
    logger.info(f"Successfully scraped {len(scraped_posts)} posts from Telegram.")
    return scraped_posts

def scrape_telegram_channel(channel_username="othcommunity"):
    """
    Master function to scrape telegram channel.
    Checks environment configuration to choose between Telethon API or BS4 Web Scraper.
    """
    api_id = os.environ.get("TELEGRAM_API_ID")
    api_hash = os.environ.get("TELEGRAM_API_HASH")
    session_str = os.environ.get("TELEGRAM_STRING_SESSION")
    
    if api_id and api_hash:
        try:
            import asyncio
            logger.info("Telethon credentials detected. Invoking Telethon scraper...")
            
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
            posts = loop.run_until_complete(
                scrape_telegram_telethon(
                    int(api_id), api_hash, session_str, channel_username, limit=50
                )
            )
            if posts:
                return posts
            logger.warning("Telethon scraping returned zero results. Falling back to BeautifulSoup scraper.")
        except Exception as e:
            logger.error(f"Error initializing or running Telethon: {e}. Falling back to BeautifulSoup.")
            
    logger.info("Invoking BeautifulSoup Web Preview Scraper...")
    return scrape_telegram_bs4(channel_username)

def clean_scrape_text(text):
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
    date_match = re.search(r'(?:Date|Dates):\s*(.*)', text, re.IGNORECASE)
    date_str = ""
    if date_match:
        date_str = date_match.group(1).strip()
    else:
        found_dates = re.findall(r'\b\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4}\b', text, re.IGNORECASE)
        if found_dates:
            date_str = found_dates[0]
            
    if not date_str:
        return None, None
        
    date_str = re.sub(r'[*_`~#|]', '', date_str).strip()
    parts = re.split(r'\s*[-–to]\s*', date_str, flags=re.IGNORECASE)
    start_date = parts[0].strip()
    end_date = parts[1].strip() if len(parts) > 1 else start_date
    return start_date, end_date

def parse_time_range(text):
    time_match = re.search(r'(?:Time|Session Time):\s*(.*)', text, re.IGNORECASE)
    time_str = ""
    if time_match:
        time_str = time_match.group(1).strip()
    else:
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
    prices = [float(p) for p in re.findall(r'\$\s*(\d+(?:\.\d{2})?)', text)]
    if prices:
        return min(prices), max(prices)
    return 0.0, 0.0

def parse_status_and_registration(text):
    text_lower = text.lower()
    if any(k in text_lower for k in ["fully booked", "closed", "registration closed", "cancelled"]):
        return "Closed", False
    return "Active", True

def merge_and_download_images(events, telegram_messages):
    """
    Merges OnePA events with Telegram messages and downloads/copies images on match.
    Also extracts unmatched Telegram messages as standalone event records.
    Modifies and returns the final events list.
    """
    merged_events = []
    matched_post_ids = set()
    unique_hashes = get_unique_image_hashes()
    
    # 1. Match OnePA events to Telegram posts
    for event in events:
        event_copy = dict(event)
        
        matched_msg = match_event_to_telegram(event_copy, telegram_messages)
        
        if matched_msg:
            matched_post_ids.add(matched_msg.get('post_id'))
            event_copy['telegram_image_urls'] = matched_msg.get('image_urls', [])
            event_copy['telegram_message_text'] = matched_msg.get('text')
            
            # Use pre-downloaded Telethon paths if available
            if matched_msg.get('image_paths'):
                local_paths = []
                for idx, old_path in enumerate(matched_msg['image_paths']):
                    if os.path.exists(old_path):
                        ext = os.path.splitext(old_path)[1] or ".jpg"
                        new_filename = f"{event_copy['event_id']}_{idx}{ext}"
                        
                        # Check mappings first
                        mapping_path = os.path.join(IMAGE_DIR, "image_mappings.json")
                        if os.path.exists(mapping_path):
                            try:
                                with open(mapping_path, "r") as f:
                                    mappings = json.load(f)
                                if new_filename in mappings:
                                    mapped_target = os.path.join(IMAGE_DIR, mappings[new_filename])
                                    if os.path.exists(mapped_target):
                                        local_paths.append(mapped_target)
                                        continue
                            except:
                                pass
                                
                        # Save/deduplicate after copying to a temporary file
                        temp_new_path = os.path.join(IMAGE_DIR, f"temp_copy_{new_filename}")
                        try:
                            import shutil
                            shutil.copy2(old_path, temp_new_path)
                            final_path = save_or_dedup_image(temp_new_path, new_filename, unique_hashes)
                            if final_path:
                                local_paths.append(final_path)
                        except Exception as copy_err:
                            logger.error(f"Error tagging telethon image {old_path} -> {new_filename}: {copy_err}")
                            if os.path.exists(temp_new_path):
                                try: os.remove(temp_new_path)
                                except: pass
                            local_paths.append(old_path)
                event_copy['telegram_image_local_paths'] = local_paths
            # Otherwise download BeautifulSoup URLs
            elif matched_msg.get('image_urls'):
                local_paths = download_images(matched_msg.get('image_urls'), event_copy['event_id'])
                event_copy['telegram_image_local_paths'] = local_paths
            else:
                event_copy['telegram_image_local_paths'] = []
        else:
            event_copy['telegram_image_urls'] = []
            event_copy['telegram_image_local_paths'] = []
            event_copy['telegram_message_text'] = None
            
        merged_events.append(event_copy)
        
    # 2. Extract unmatched Telegram messages as standalone events
    unmatched_messages = [msg for msg in telegram_messages if msg.get('post_id') not in matched_post_ids]
    logger.info(f"Extracting {len(unmatched_messages)} unmatched Telegram posts as new activities...")
    
    for msg in unmatched_messages:
        text = msg.get('text', '')
        if not text or len(text.strip()) < 10: # Ignore very short/empty messages
            continue
            
        post_id = msg.get('post_id', '')
        channel_name = "telegram"
        msg_id = post_id
        if '/' in post_id:
            parts = post_id.split('/')
            channel_name = parts[0]
            msg_id = parts[1]
            
        event_id = f"tele_scrape_{channel_name}_{msg_id}"
        event_name = parse_event_name(text)
        organiser = parse_organiser(text)
        venue = parse_venue(text)
        start_date, end_date = parse_date_range(text)
        start_time, end_time = parse_time_range(text)
        min_price, max_price = parse_prices(text)
        status, reg_open = parse_status_and_registration(text)
        
        # Get url
        url_match = re.search(r'https?://[^\s]+', text)
        url = url_match.group(0) if url_match else f"https://t.me/{channel_name}/{msg_id}"
        
        # Handle images for unmatched messages
        local_paths = []
        if msg.get('image_paths'):
            for idx, old_path in enumerate(msg['image_paths']):
                if os.path.exists(old_path):
                    ext = os.path.splitext(old_path)[1] or ".jpg"
                    new_filename = f"{event_id}_{idx}{ext}"
                    
                    # Check mappings first
                    mapping_path = os.path.join(IMAGE_DIR, "image_mappings.json")
                    if os.path.exists(mapping_path):
                        try:
                            with open(mapping_path, "r") as f:
                                mappings = json.load(f)
                            if new_filename in mappings:
                                mapped_target = os.path.join(IMAGE_DIR, mappings[new_filename])
                                if os.path.exists(mapped_target):
                                    local_paths.append(mapped_target)
                                    continue
                        except:
                            pass
                            
                    temp_new_path = os.path.join(IMAGE_DIR, f"temp_copy_{new_filename}")
                    try:
                        import shutil
                        shutil.copy2(old_path, temp_new_path)
                        final_path = save_or_dedup_image(temp_new_path, new_filename, unique_hashes)
                        if final_path:
                            local_paths.append(final_path)
                    except Exception as copy_err:
                        logger.error(f"Error copying telethon image for unmatched event {event_id}: {copy_err}")
                        if os.path.exists(temp_new_path):
                            try: os.remove(temp_new_path)
                            except: pass
        elif msg.get('image_urls'):
            local_paths = download_images(msg.get('image_urls'), event_id)
            
        record = {
            'event_id': event_id,
            'event_name': event_name,
            'organiser_name': organiser,
            'organiser_profile_name': channel_name,
            'description': text,
            'physical_venue': venue,
            'physical_address': venue,
            'event_start': None,
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
            'source': f"telegram_scrape_{channel_name}",
            'telegram_image_local_paths': local_paths
        }
        
        merged_events.append(record)
        
    # 3. Clean up temp files and telethon original files to keep IMAGE_DIR clean
    try:
        if os.path.exists(IMAGE_DIR):
            for filename in os.listdir(IMAGE_DIR):
                if filename.startswith("telethon_") or filename.startswith("temp_"):
                    file_path = os.path.join(IMAGE_DIR, filename)
                    if os.path.isfile(file_path):
                        os.remove(file_path)
    except Exception as e:
        logger.warning(f"Failed to clean up temporary/telethon images: {e}")
        
    return merged_events
