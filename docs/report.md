# Report

## Part 1
In Part 1, describe how your pipeline works from start to finish. Compare the performance of both serial and parallel downloading. Report which approach was faster, and explain any trade-offs you observed, such as added complexity or system load. Provide script snippets as needed.

How the code works
File system:
Input files
`video_urls.txt` contains YouTube video urls. 
Output files
`docs` contains this report and the data analysis report.
`logs` contains download `logs`.
`audio_output` contains all the downloaded files and the downloaded json metadata. 
`metadata_output` contains all of the metadata comined into a csv. 

Processing scripts:
`extract_videos.py` contains common functions used by both the serial and parallel processing implementations.
`serial_processing.py` contains all the functions unique to the serial processing implementation.
`parallel_processing.py` contains all the functions unqiue to the parallel processing implementation.
`main.py` brings all the main functions together. This is the main file to read. 


The `video_urls.txt` file contains 10 YouTube urls, each on a separate line. This file is read using the `load_txt` function which reads each line as a new item in the list, with whitespace removed. 

This list of urls is passed to either the `download_serial` function or the `download_parallel` function.

Download serial
Using the `log_and_print` function which allows a message to be logged to the log file (using the logging module) and printed to the console, the function tells a user that it has begun.
It marks a `start_time` using the `time` module so that we can see how long it takes and starts a counter for `total_sucessful` and `total_failed` downloads. 
For each url in the `urls` list it tries to `download_youtube_audio_with_metadata`. 

The `download_youtube_audio_with_metadata` takes the url, a set number of retries (if something fails it will try three times by default) and an initial delay (used for exponential backoff). It then passes this url to the `get_video_info` function 


