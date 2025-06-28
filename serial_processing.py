import certifi
import os
import json
import logging
import time
from extract_videos import get_video_info, extract_metadata


OUTPUT_DIR = "audio_output"
LOGS_DIR = "logs"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)
os.environ["SSL_CERT_FILE"] = certifi.where()

logger = logging.getLogger("Serial Processing")
logging.basicConfig(filename=f"{LOGS_DIR}/logs", encoding='utf-8', level=logging.DEBUG)

def save_metadata(metadata: dict, title: str) -> str:
    """Save metadata as a JSON file and return the path."""
    safe_title = "".join(c for c in title if c.isalnum() or c in (" ", "_", "-")).rstrip()
    file_path = os.path.join(OUTPUT_DIR, f"{safe_title}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    return file_path

def download_youtube_audio_with_metadata(url: str):
    """Main function to download audio and save metadata."""
    print(f"\n🎵 Downloading: {url}")
    logger.info(f"Downloading: {url}")
    successful = 0
    failed = 0
    try:
        info = get_video_info(url)
        metadata = extract_metadata(info)
        json_path = save_metadata(metadata, metadata["title"])
        print(f"✅ Done: {metadata['title']}\n📄 Metadata saved: {json_path}")
        logger.info(f"Downloaded: {metadata['title']} - Metadata saved to {json_path}")
        successful += 1
    except Exception as e:
        print(f"❌ Failed to download: {url}\n   Error: {e}")
        logger.error(f"Failed to download {url}: {e}")
        failed += 1
    return successful, failed
        
def download_serial(urls: list):
    """Download videos one by one."""
    start_time = time.time()
    for url in urls:
        successful, failed = download_youtube_audio_with_metadata(url)
        end_time = time.time()
    print(f"\n🎯 Download Summary:")
    print(f"   ✅ Successful: {successful}")
    print(f"   ❌ Failed: {failed}")
    print(f"   ⏱⌛ Total time: {end_time - start_time:.2f} seconds")
    logger.info(f"Time taken to download {len(urls)} videos: {end_time - start_time:.2f} seconds.")

# if __name__ == "__main__":
#     youtube_urls = load_txt("video_urls.txt")
#     start_time = time.time()
#     for url in youtube_urls:
#         download_youtube_audio_with_metadata(url)
#         end_time = time.time()
#     print(f"⌛ Time taken: {end_time - start_time:.2f} seconds\n")
#     logger.info(f"Time taken to download {len(youtube_urls)} videos: {end_time - start_time:.2f} seconds.")