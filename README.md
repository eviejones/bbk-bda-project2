# bbk-bda-project2
Project 2 for Big Data Analytics

## File structure:
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

## To run
1. Install requirements in your venv:
`pip install -r requirements.txt`
2. Add video urls to `video_urls.txt`
3. Then you should be able to run `main.py`
4. For analysis run `analysis/data_analysis.ipynb`