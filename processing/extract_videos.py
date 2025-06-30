import yt_dlp
import certifi
import os

#TODO check null things are handled properly, suspicuous about artist
#TODO validate txt reading function
OUTPUT_DIR = "audio_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.environ["SSL_CERT_FILE"] = certifi.where()

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
    data = [line.strip() for line in data if line.strip()]
    return data

def get_video_info(url: str, download: bool = True) -> dict:
    """Extract video info and optionally download the audio without warnings.
    
    Args:
        url (str): The URL of the YouTube video.
        download (bool): Whether to download the audio. Defaults to True.

    Returns:
        dict: A dictionary containing video metadata.
    """
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
    """Filter and return relevant metadata fields, including derived values.
    
    Args:
        info (dict): The video information dictionary returned by yt-dlp.
    
    Returns:
        dict: A dictionary containing filtered metadata.
    """
    upload_date = info.get("upload_date")
    tags = info.get("tags") or [] # Ensure it's a list, even if empty

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
        "tags": tags,  
        "duration_seconds": info.get("duration"),
        "upload_date": info.get("upload_date"),
        "view_count": info.get("view_count"),
        "like_count": info.get("like_count"),
        "webpage_url": info.get("webpage_url"),

        # Derived fields
        "year_uploaded": int(upload_date[:4]) if upload_date else None,
        "tag_count": len(tags)}