import os
import re
import requests
import logging
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Directory inside the container where images will be saved
IMAGE_DIR = "/opt/airflow/data/telegram_images"

def scrape_telegram_channel(channel_username="othcommunity"):
    """
    Scrapes the public preview of a Telegram channel.
    Returns a list of dicts: [{'post_id': str, 'text': str, 'image_url': str, 'date': str}]
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
        # Extract post ID (e.g. "othcommunity/1234" from data-post)
        post_id = msg.get('data-post', '')
        
        # Extract text
        text_elem = msg.find('div', class_='tgme_widget_message_text')
        text = text_elem.get_text(separator=" ").strip() if text_elem else ""
        
        # Extract timestamp
        time_elem = msg.find('time', class_='time')
        timestamp = time_elem.get('datetime', '') if time_elem else ""
        
        # Extract image URL from style attribute
        image_url = None
        photo_elem = msg.find('a', class_='tgme_widget_message_photo_wrap')
        if photo_elem and 'style' in photo_elem.attrs:
            style_content = photo_elem['style']
            match = re.search(r"url\(['\"]?(.*?)['\"]?\)", style_content)
            if match:
                image_url = match.group(1)
        
        if text or image_url:
            scraped_posts.append({
                'post_id': post_id,
                'text': text,
                'image_url': image_url,
                'date': timestamp
            })
            
    logger.info(f"Successfully scraped {len(scraped_posts)} posts from Telegram.")
    return scraped_posts

def download_image(image_url, event_id):
    """
    Downloads an image from image_url and saves it to IMAGE_DIR as {event_id}.jpg.
    Returns the local path if successful, otherwise None.
    """
    if not image_url:
        return None
        
    try:
        os.makedirs(IMAGE_DIR, exist_ok=True)
        local_filename = f"{event_id}.jpg"
        local_path = os.path.join(IMAGE_DIR, local_filename)
        
        # Check if already downloaded to prevent redundant downloads
        if os.path.exists(local_path):
            logger.info(f"Image for event {event_id} already exists locally. Skipping download.")
            return local_path
            
        logger.info(f"Downloading image for event {event_id} from {image_url}...")
        response = requests.get(image_url, stream=True, timeout=15)
        response.raise_for_status()
        
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                
        logger.info(f"Successfully downloaded image to {local_path}")
        return local_path
    except Exception as e:
        logger.error(f"Failed to download image for event {event_id} from {image_url}: {e}")
        return None

def clean_text(text):
    """Clean text by lowercasing and removing punctuation/special chars."""
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', ' ', text)
    return " ".join(text.split())

def match_event_to_telegram(event, telegram_messages):
    """
    Matches a single OnePA event to the best Telegram post.
    Returns the matched Telegram post or None.
    """
    event_id = str(event.get('event_id', ''))
    event_title = event.get('title', '')
    event_date_str = event.get('start_date', '')
    
    if not event_title:
        return None
        
    cleaned_title = clean_text(event_title)
    
    # Define stopwords to filter out from title keywords
    stop_words = {
        'the', 'a', 'in', 'at', 'on', 'of', 'and', 'for', 'with', 'by', 'is', 'to', 'from',
        'our', 'tampines', 'hub', 'oth', 'community', 'club', 'cc', 'rn', 'rc', 'ig',
        'session', 'class', 'programme', 'program', 'interest', 'group', 'weekly', 'monthly',
        'jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec',
        'january', 'february', 'march', 'april', 'june', 'july', 'august', 'september', 'october', 'november', 'december'
    }
    keywords = [w for w in cleaned_title.split() if w not in stop_words and len(w) > 2]
    
    best_match = None
    best_score = 0.0
    
    # Extract date tokens for validation
    days = re.findall(r'\b\d{1,2}\b', event_date_str)
    months = re.findall(r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\b', event_date_str, re.IGNORECASE)
    
    for msg in telegram_messages:
        msg_text = msg.get("text", "")
        if not msg_text:
            continue
            
        cleaned_msg = clean_text(msg_text)
        
        # 1. Explicit Matching:
        # Check if the post mentions the exact event ID or has a link containing the event ID
        if event_id in msg_text or event_id in msg.get("image_url", ""):
            logger.info(f"Explicit ID Match found for Event {event_id} in Telegram post {msg.get('post_id')}")
            return msg
            
        # 2. Fuzzy Keyword and Date Matching:
        if not keywords:
            continue
            
        matches = sum(1 for kw in keywords if kw in cleaned_msg)
        overlap_score = matches / len(keywords)
        
        # Verify date overlap (day & month name)
        date_matched = False
        if days and months:
            for day in days:
                for month in months:
                    month_abbr = month[:3].lower()
                    if (f"{day} {month_abbr}" in cleaned_msg) or (f"{month_abbr} {day}" in cleaned_msg):
                        date_matched = True
                        break
                if date_matched:
                    break
                    
        final_score = overlap_score
        # Give a substantial boost if the date matches to prevent false positives
        if date_matched:
            final_score += 0.4
            
        # Require a threshold of 0.5 (e.g. 50% keyword match + date match, or high keyword match)
        if final_score >= 0.5 and final_score > best_score:
            best_score = final_score
            best_match = msg
            
    if best_match:
        logger.info(f"Fuzzy Match found for Event {event_id} ('{event_title}') with score {best_score:.2f}")
    return best_match

def merge_and_download_images(events, telegram_messages):
    """
    Merges OnePA events with Telegram messages and downloads images on match.
    Modifies and returns the events list.
    """
    merged_events = []
    
    for event in events:
        # Make a copy to avoid side-effects
        event_copy = dict(event)
        
        matched_msg = match_event_to_telegram(event_copy, telegram_messages)
        
        if matched_msg:
            # Set metadata
            event_copy['telegram_image_url'] = matched_msg.get('image_url')
            event_copy['telegram_message_text'] = matched_msg.get('text')
            
            # Conditionally download image only if a match is established
            if matched_msg.get('image_url'):
                local_path = download_image(matched_msg.get('image_url'), event_copy['event_id'])
                event_copy['telegram_image_local_path'] = local_path
            else:
                event_copy['telegram_image_local_path'] = None
        else:
            # Initialize empty fields for schema consistency
            event_copy['telegram_image_url'] = None
            event_copy['telegram_image_local_path'] = None
            event_copy['telegram_message_text'] = None
            
        merged_events.append(event_copy)
        
    return merged_events
