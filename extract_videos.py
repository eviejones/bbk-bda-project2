import yt_dlp
import certifi
import os
import logging

OUTPUT_DIR = "audio_output"
LOGS_DIR = "logs"
MAX_WORKERS = 5
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)
os.environ["SSL_CERT_FILE"] = certifi.where()

logger = logging.getLogger("Extract Videos")
logging.basicConfig(filename=f"{LOGS_DIR}/logs", encoding='utf-8', level=logging.DEBUG)

def load_txt(filename: str) -> list:
    """
    Loads a txt file and returns its content as a list of strings.

    Args:
        filename (str): The path to the txt file.

    Returns:
        list: A list with each line of the txt file as an element.

    Example:
        load_text("video_urls.txt")
    """
    with open(filename, mode="r", newline="") as file:
        data = file.readlines()
    data = [line.strip() for line in data if line.strip()]  # Remove empty
    return data

def get_video_info(url: str, download: bool = True) -> dict:
    """Extract video info and optionally download the audio without warnings."""
    ydl_opts = {
        'format': 'bestaudio[ext=m4a]/bestaudio',
        'outtmpl': os.path.join(OUTPUT_DIR, '%(title)s.%(ext)s'),
        'noplaylist': True,
        'quiet': True,          # suppress general output
        'no_warnings': True,    # suppress warnings
        'skip_download': not download,
        'postprocessors': [],
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        return ydl.extract_info(url, download=download)
    
def extract_metadata(info: dict) -> dict:
    """Filter and return relevant metadata fields, including derived values."""
    upload_date = info.get("upload_date")
    tags = info.get("tags") or []

    return {
        "id": info.get("id"),
        "title": info.get("title"),
        "uploader": info.get("uploader"),
        "uploader_id": info.get("uploader_id"),
        "channel": info.get("channel"),
        "track": info.get("track"),
        "artist": info.get("artist"),
        "album": info.get("album"),
        "description": info.get("description"),
        "tags": tags,  # Ensure it's a list
        "duration_seconds": info.get("duration"),
        "upload_date": upload_date,
        "view_count": info.get("view_count"),
        "like_count": info.get("like_count"),
        "webpage_url": info.get("webpage_url"),

        # Derived fields
        "year_uploaded": int(upload_date[:4]) if upload_date else None,
        "tag_count": len(tags)}