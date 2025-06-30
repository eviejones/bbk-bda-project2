"""Loads the list of urls and downloads data both serially and in parallel. The combines this metadata into a single file."""

from processing.extract_videos import load_txt
from processing.parallel_processing import download_parallel
from processing.serial_processing import download_serial
from processing.combine_metadata import combine_metadata
from processing.config import OUTPUT_DIR  # "audio_output"

if __name__ == "__main__":
    youtube_urls = load_txt("video_urls.txt")
    download_serial(youtube_urls)
    download_parallel(youtube_urls)
    combine_metadata(OUTPUT_DIR)
