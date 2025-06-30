import certifi
import os
import json
import logging
import time
from .extract_videos import get_video_info, extract_metadata
from .config import OUTPUT_DIR, LOGS_DIR

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)
os.environ["SSL_CERT_FILE"] = certifi.where()

logger = logging.getLogger("Serial Processing")
logging.basicConfig(filename=f"{LOGS_DIR}/logs", encoding='utf-8', level=logging.DEBUG)

def log_and_print(message: str, level: str = "info") -> None:
    """Log message and print to console simultaneously.
    
    Args:
        message (str): The message to log and print.
        level (str): The logging level (default is "info").
    """
    print(message)
    getattr(logger, level.lower())(message)

def save_metadata(metadata: dict, title: str) -> str:
    """Save metadata as a JSON file and return the path.
    
    Args:
        metadata (dict): The metadata to save.
        title (str): The title to use for the filename.
    
    Returns:
        str: The path to the saved JSON file."""
    safe_title = "".join(c for c in title if c.isalnum() or c in (" ", "_", "-")).rstrip()
    file_path = os.path.join(OUTPUT_DIR, f"{safe_title}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    return file_path

def download_youtube_audio_with_metadata(url: str, max_retries: int = 3, initial_delay: int = 5) -> tuple:
    """
    Main function to download audio and save metadata with retry logic.

    Args:
        url (str): The URL of the YouTube video to download.
        max_retries (int): The maximum number of times to retry a failed download.
        initial_delay (int): The initial delay in seconds before the first retry.

    Returns:
        tuple: A tuple containing (successful_count, failed_count) for this attempt.
    """
    log_and_print(f"🎵 Attempting to download: {url}", "info")
    successful = 0
    failed = 0
    retries = 0

    while retries <= max_retries:
        try:
            info = get_video_info(url)
            metadata = extract_metadata(info)
            json_path = save_metadata(metadata, metadata["title"])
            log_and_print(f"✅ Done: {metadata['title']}\n📄 Metadata saved: {json_path}", "info")
            successful += 1
            return successful, failed # Success, exit the loop
        except Exception as e:
            log_and_print(f"❌ Failed to download: {url}\n   Error: {e}", "error")
            retries += 1
            if retries <= max_retries:
                delay = initial_delay * (2 ** (retries - 1)) # Exponential backoff
                log_and_print(f"Retrying download for {url} in {delay} seconds (Attempt {retries}/{max_retries})...", "warning")
                time.sleep(delay)
            else:
                log_and_print(f"🛑 Max retries ({max_retries}) exceeded for {url}. Giving up.", "error")
                failed += 1
                return successful, failed # All retries failed

    return successful, failed # Should not be reached

def download_serial(urls: list) -> None:
    """Download videos one by one in a serial manner.
    
    Args:
        urls (list): List of YouTube video URLs to download.
    """
    log_and_print("=== Starting serial downloads ===", "info")
    start_time = time.time()
    total_successful = 0
    total_failed = 0
    for url in urls:
        successful, failed = download_youtube_audio_with_metadata(url)
        total_successful += successful
        total_failed += failed
    end_time = time.time()
    log_and_print(f"\n🎯 Download Summary:\n   ✅ Successful: {total_successful}\n   ❌ Failed: {total_failed}\n   ⌛ Time taken to download {len(urls)}: {end_time - start_time:.2f} seconds", "info")