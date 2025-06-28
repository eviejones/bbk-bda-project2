import yt_dlp
import certifi
import os
import json

OUTPUT_DIR = "audio_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.environ["SSL_CERT_FILE"] = certifi.where()

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
        "tag_count": len(tags),
    }


def save_metadata_to_file(metadata: dict, title: str) -> str:
    """Save metadata as a JSON file and return the path."""
    safe_title = "".join(c for c in title if c.isalnum() or c in (" ", "_", "-")).rstrip()
    file_path = os.path.join(OUTPUT_DIR, f"{safe_title}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    return file_path

def download_youtube_audio_with_metadata(url: str):
    """Main function to download audio and save metadata."""
    print(f"\n🎵 Downloading: {url}")
    try:
        info = get_video_info(url)
        metadata = extract_metadata(info)
        json_path = save_metadata_to_file(metadata, metadata["title"])
        print(f"✅ Done: {metadata['title']}\n📄 Metadata: {json_path}")
    except Exception as e:
        print(f"❌ Failed to download: {url}\n   Error: {e}")

if __name__ == "__main__":
    youtube_urls = [
        "https://youtu.be/4D7u5KF7SP8?si=Y_S0k_5MbdW5mKI2",
        "https://youtu.be/Q3Bp1QVVieM?si=4BlPs5dpK3Y-iPYr",
        "https://youtu.be/5m4ZkEqQrn0?si=7DfUDfMGKkbktUDi"
    ]

    for url in youtube_urls:
        download_youtube_audio_with_metadata(url)
