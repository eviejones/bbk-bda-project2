"""Loads the list of urls and downloads data both serially and in parallel. The combines this metadata into a single file."""
from extract_videos import load_txt
from parallel_processing import download_parallel
from serial_processing import download_serial
from combine_metadata import combine_metadata

if __name__ == "__main__":
    youtube_urls = load_txt("video_urls.txt")
    download_serial(youtube_urls)
    download_parallel(youtube_urls)
    combine_metadata("audio_output")