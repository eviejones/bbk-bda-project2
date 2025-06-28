import certifi
import os
import json
import logging
import time
from extract_videos import get_video_info, extract_metadata

#TODO add in retry logic for failed downloads
#TODO complete doc strings
OUTPUT_DIR = "audio_output"
LOGS_DIR = "logs"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)
os.environ["SSL_CERT_FILE"] = certifi.where()

logger = logging.getLogger("Serial Processing")
logging.basicConfig(filename=f"{LOGS_DIR}/logs", encoding='utf-8', level=logging.DEBUG)

def log_and_print(message: str, level: str = "info"):
    """Log message and print to console simultaneously."""
    print(message)
    getattr(logger, level.lower())(message)

def save_metadata(metadata: dict, title: str) -> str:
    """Save metadata as a JSON file and return the path."""
    safe_title = "".join(c for c in title if c.isalnum() or c in (" ", "_", "-")).rstrip()
    file_path = os.path.join(OUTPUT_DIR, f"{safe_title}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    return file_path

def download_youtube_audio_with_metadata(url: str):
    """Main function to download audio and save metadata."""
    log_and_print(f"🎵 Downloading: {url}", "info")
    successful = 0
    failed = 0
    try:
        info = get_video_info(url)
        metadata = extract_metadata(info)
        json_path = save_metadata(metadata, metadata["title"])
        log_and_print(f"✅ Done: {metadata['title']}\n📄 Metadata saved: {json_path}", "info")
        successful += 1
    except Exception as e:
        log_and_print(f"❌ Failed to download: {url}\n   Error: {e}", "error")
        failed += 1
    return successful, failed  

def download_serial(urls: list):
    """Download videos one by one."""
    log_and_print("Starting serial downloads...", "info")
    start_time = time.time()
    total_successful = 0
    total_failed = 0
    for url in urls:
        successful, failed = download_youtube_audio_with_metadata(url)
        total_successful += successful
        total_failed += failed
    end_time = time.time()
    log_and_print(f"\n🎯 Download Summary:\n   ✅ Successful: {total_successful}\n   ❌ Failed: {total_failed}\n   ⌛ Time taken to download {len(urls)}: {end_time - start_time:.2f} seconds", "info")