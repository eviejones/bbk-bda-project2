import certifi
import os
import json
import logging
import threading
from threading import Lock
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from .extract_videos import get_video_info, extract_metadata


OUTPUT_DIR = "audio_output"
LOGS_DIR = "logs"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)
os.environ["SSL_CERT_FILE"] = certifi.where()

# Logging setup 
logger = logging.getLogger("Parallel Processing")
logging.basicConfig(filename=f"{LOGS_DIR}/logs", encoding='utf-8', level=logging.DEBUG)
output_lock = Lock() # For thread-safe printing and logging

def log_and_print(message: str, level: str = "info"):
    """Thread-safe function to log message and print to console simultaneously."""
    with output_lock:
        print(message)
        getattr(logger, level.lower())(message)

def save_metadata_parallel(metadata: dict, title: str) -> str:
    """Save metadata as a JSON file and return the path."""
    safe_title = "".join(c for c in title if c.isalnum() or c in (" ", "_", "-")).rstrip()
    file_path = os.path.join(OUTPUT_DIR, f"{safe_title}.json")
    
    # Must use lock to ensure log output is thread-safe
    with output_lock:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
    return file_path

def download_youtube_audio_with_metadata_parallel(url: str):
    """Main function to download audio and save metadata - now thread-safe."""
    thread_id = threading.current_thread().name
    log_and_print(f"🎵 [{thread_id}] Downloading: {url}", "info")

    try:
        info = get_video_info(url)
        metadata = extract_metadata(info)
        json_path = save_metadata_parallel(metadata, metadata["title"])
        log_and_print(f"✅ [{thread_id}] Done: {metadata['title']}\n📄 Metadata saved: {json_path}", "info")
        return {"status": "success", "url": url, "title": metadata["title"]}
        
    except Exception as e:
        log_and_print(f"❌ [{thread_id}] Failed to download: {url}\n   Error: {e}", "error")
        return {"status": "error", "url": url, "error": str(e)}

def download_parallel(urls: list[str], max_workers: int =5):
    """Download videos using ThreadPoolExecutor."""
    log_and_print(f"Starting parallel downloads with {max_workers} threads...", "info")
    results = []
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="Downloader Thread") as executor:
        # Submit all tasks
        future_to_url = {
            executor.submit(download_youtube_audio_with_metadata_parallel, url): url 
            for url in urls
        }
        
        # Process completed tasks as they finish rather than waiting for all to finish
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                log_and_print(f"❌ Unexpected error for {url}: {e}", "error")
                results.append({"status": "error", "url": url, "error": str(e)})
    
    end_time = time.time()
    
    # Summary
    successful = sum(1 for r in results if r["status"] == "success")
    failed = len(results) - successful
    
    log_and_print(f"\n🎯 Download Summary:\n   ✅ Successful: {successful}\n   ❌ Failed: {failed}\n   ⌛ Time taken to download {len(urls)}: {end_time - start_time:.2f} seconds", "info")
