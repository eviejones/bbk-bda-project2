# Report

## Part 1
In Part 1, describe how your pipeline works from start to finish. Compare the performance of both serial and parallel downloading. Report which approach was faster, and explain any trade-offs you observed, such as added complexity or system load. Provide script snippets as needed.

## How the code works

This project implements a pipeline for downloading YouTube video audio and extracting associated metadata, offering both serial and parallel processing capabilities. The core logic is distributed across several Python scripts: `extract_videos.py` contains shared functionalities like video information retrieval and metadata extraction; `serial_processing.py` and `parallel_processing.py` contain the implementations for sequential and concurrent downloading (using threads); `combine_metadata.py` handles the aggregation, validation, and cleaning of the individual JSON metadata files into a single CSV; and `main.py` orchestrats the workflow from URL ingestion to output of combined csv.

### File system
Processing scripts:
`processing/extract_videos.py` contains common functions used by both the serial and parallel processing implementations.
`processing/serial_processing.py` contains all the functions unique to the serial processing implementation.
`processing/parallel_processing.py` contains all the functions unqiue to the parallel processing implementation.
`processing/combine_metadata.py` contains functions to combine the metadata into one csv. It contains validation and cleaning. 
`main.py` brings all the main functions together. This is the main file to run. 

Additional files:
`video_urls.txt` contains YouTube video urls. 
Output files
`docs` contains this report and the data analysis report.
`logs` contains download `logs`.
`audio_output` contains all the downloaded files and the downloaded json metadata. 
`metadata_output` contains all of the metadata comined into a csv. 
`analysis` contains the analysis for part 3. 
`processing/config.py` contains all the constants used across files. It contains all the filepaths used and the retry logic.

### The process
The `video_urls.txt` file contains 10 YouTube urls, each on a separate line. Inside `main.py` this file is read using the `load_txt` function which reads each line as a new item in the list, with whitespace removed. 

This list of urls is passed to either the `download_serial` function or the `download_parallel` function. The metadata output of these files is then combined using the `combine_metadata` function. This data is then analysed in the `data_analysis.ipynb` notebook. The output of this can be seen in `docs/data_analysis.pdf`.

### Download serial
Using the `log_and_print` function which allows a message to be logged to the log file (using the logging module) and printed to the console, the function tells a user that it has begun. It marks a `start_time` using the `time` module so that we can see how long it takes and starts a counter for `total_sucessful` and `total_failed` downloads. 
For each url in the `urls` list it tries to `download_youtube_audio_with_metadata`. 

The `download_youtube_audio_with_metadata` takes the url, a set number of retries (if something fails it will try three times by default) and an initial delay (used for exponential backoff). It then passes this url to the `get_video_info` function.

The `get_video_info` uses the `yt_dlp` package to download information about a YouTube video give a url. It sets options `yt_opt` and passes these to the `YoutubeDL` with an alias `ydl` and returns the requested information. 

Once the video has downloaded the script then uses `extract_metadata` to get key information about the video. This information is saved in a JSON file using `save_metadata` and then the file path is logged to the logs and printed to the console to let the user know this is completed. 

### Download parallel
For the parallel processing version, I have made use of ThreadpoolExecuter and the `as_completed` function, which uses a pool of threads (set to 5 `MAX_WORKERS` as default). The ThreadpoolExecuter is particularly well suited to I/O tasks such as downloading data. 

When I first ran this I had issues with the logging output not working properly so I had to implement `Lock()` for my `log_and_print` function. Using lock adds complexity because all the shared resources (printing, logging and writing metadata) had to be identified and protected to prevent race conditions and corruption. Parallel processing is inherentially harder to debug so it was crucial to include good error handling and logging. 

When writing it initially, I submitted the tasks in two batches (`get_video_info`, `extract_metadata`) but realised that `extract_metadata` relies on `get_video_info` to have completed. Instead each url is treated as a task where the task is finished when the video has been downloaded and the metadata saved to a file. However, due to the use of `as_completed` the script starts processing the results of the completed downloads rather than waitint until all submited tasks have completed. This is efficient because some videos take longer to download than others. 

## Comparison
Serial download time taken to download 10 videos: 59.26 seconds.
Parallel download time taken to download 10 videos with 5 workers: 38.56 seconds.
The parallel was 20.7 seconds faster. 

In serial processing, each video download had to complete before the next one beguns, leading to significant idle time while waiting for network responses. 

Interestingly increasing the max_workers to 10, only increases the speed to 34.74 seconds. There is a danger with increasing the number of max_workers as it can overload the internet bandwidth and would slow down the processing. Though not fully tested, there is an indication that increasing the max_workers has diminishing returns. 

As the serial processing deals with one video at a time, this means the time complexity is the sum of the total time taken to process a video. Additionally only one video's data is held in memory at a time. The space complexity is therefore less than when using the threadpool. The parallel processing script uses more memory and videos may be held in memory concurrently. For both, the amount of memory stored in the outputs is proportional to the number of videos processed. 

With parallel processing, execution time is reduced but extra complexity is added in relation to resource management and error handling. 

# Part 2 

### Combining metadata
The `combine_metadata.py` has a list of required columns and their expected data types. For each JSON file in the `audio_output` folder it loads the data and adds it to a pandas dataframe. This dataframe is then cleaned:
- It uses `validate_dataframe` to check that only the expected columns are present and they are all the expected data type. If they are not this is printed and logged. 
- For numeric columns, N/As are filled with 0
- Those with a duplicate id are dropped
- `upload_date` is turned to a `pd.Timestamp` for better processing and visualisation e.g. if you were to create a time series. 
- An additional column for a legible title is added. This is not a perfect title but works for most instances. The logic is that most YouTube videos have a title with a - or | in the name e.g. `Hound dog | Elvis Presley`. It splits the title at this point and reduces the title to the first five words. This is used for better visualisation.

This dataframe is then saved to csv. 

# Part 3
The file `analysis/data_analysis.ipynb` contains all the required analysis code in Python as well as Spark. It uses matplotlib for the visualisation. 

As mentioned in the assignment file, there didn't seem to be much performance difference between pandas and spark. I use pandas in my daily work so prefer it but it was useful to learn the spark syntax for if I were to work with larger datasets. 


# Reflection
This was a useful exercise in how to use threads for I/O heavy tasks. It would be interesting to test how the current implementation works with a larger number of videos to download. I struggled a bit with implementing api retry logic in parallel processing, even though I am familiar with it for calling API serially. I also struggled with i


