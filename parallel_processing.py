import certifi
import os
import json
import logging
import threading
from threading import Lock
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from extract_videos import get_video_info, extract_metadata


OUTPUT_DIR = "audio_output"
LOGS_DIR = "logs"
MAX_WORKERS = 5
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)
os.environ["SSL_CERT_FILE"] = certifi.where()

# Logging setup 
logger = logging.getLogger("Extract Videos")
logging.basicConfig(filename=f"{LOGS_DIR}/logs", encoding='utf-8', level=logging.DEBUG)
print_lock = Lock()  # For thread-safe console output
log_lock = Lock()   # For thread-safe logging to file

def thread_safe_print(message):
    """Thread-safe printing to avoid garbled output."""
    with print_lock:
        print(message)

def save_metadata_parallel(metadata: dict, title: str) -> str:
    """Save metadata as a JSON file and return the path."""
    safe_title = "".join(c for c in title if c.isalnum() or c in (" ", "_", "-")).rstrip()
    file_path = os.path.join(OUTPUT_DIR, f"{safe_title}.json")
    
    # Use file_lock if multiple threads might write files with similar names
    with log_lock:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
    return file_path

def download_youtube_audio_with_metadata_parallel(url: str):
    """Main function to download audio and save metadata - now thread-safe."""
    thread_id = threading.current_thread().name
    thread_safe_print(f"\n🎵 [{thread_id}] Downloading: {url}")
    logger.info(f"[{thread_id}] Downloading: {url}")
    
    try:
        info = get_video_info(url)
        metadata = extract_metadata(info)
        json_path = save_metadata_parallel(metadata, metadata["title"])
        
        thread_safe_print(f"✅ [{thread_id}] Done: {metadata['title']}\n📄 Metadata saved: {json_path}")
        logger.info(f"[{thread_id}] Downloaded: {metadata['title']} - Metadata saved to {json_path}")
        
        # update_progress()
        return {"status": "success", "url": url, "title": metadata["title"]}
        
    except Exception as e:
        error_msg = f"❌ [{thread_id}] Failed to download: {url}\n   Error: {e}"
        thread_safe_print(error_msg)
        logger.error(f"[{thread_id}] Failed to download {url}: {e}")
        
        # update_progress()
        return {"status": "error", "url": url, "error": str(e)}

def download_parallel(urls, max_workers=MAX_WORKERS):
    """Download videos using ThreadPoolExecutor."""
    
    thread_safe_print(f"Starting parallel downloads with {max_workers} threads...")
    
    results = []
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_url = {
            executor.submit(download_youtube_audio_with_metadata_parallel, url): url 
            for url in urls
        }
        
        # Process completed tasks as they finish
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                thread_safe_print(f"❌ Unexpected error for {url}: {e}")
                results.append({"status": "error", "url": url, "error": str(e)})
    
    end_time = time.time()
    
    # Summary
    successful = sum(1 for r in results if r["status"] == "success")
    failed = len(results) - successful
    
    thread_safe_print(f"\n🎯 Download Summary:")
    thread_safe_print(f"   ✅ Successful: {successful}")
    thread_safe_print(f"   ❌ Failed: {failed}")
    thread_safe_print(f"   ⌛ Total time: {end_time - start_time:.2f} seconds")


# def get_info_parallel(urls: list[str], download: bool = True) -> dict:
#     start_time = time.time()
#     results = []
#     with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
#         # Submit all tasks
#         future_to_url = {
#             executor.submit(get_video_info, url): url 
#             for url in urls
#         }
        
#         # Process completed tasks as they finish
#         for future in as_completed(future_to_url):
#             url = future_to_url[future]
#             try:
#                 result = future.result()
#                 results.append(result)
#             except Exception as e:
#                 results.append({"status": "error", "url": url, "error": str(e)})

#     end_time = time.time()
#     print(f"Processed {len(urls)} URLs in {end_time - start_time:.2f} seconds")


# if __name__ == "__main__":
#     youtube_urls = load_txt("video_urls.txt")
#     download_parallel(youtube_urls)
