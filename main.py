from extract_videos import load_txt
from parallel_processing import download_parallel
from serial_processing import download_serial
from extract_metadata import extract_metadata
youtube_urls = load_txt("video_urls.txt")

download_serial(youtube_urls)
download_parallel(youtube_urls)
extract_metadata("audio_output")