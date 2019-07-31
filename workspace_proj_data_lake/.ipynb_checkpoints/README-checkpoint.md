## Project Summary

The purpose of this project is to create generate similar data structure as database using Spark and AWS platform for Sparkify to do analytical summaries for the user activities. The overall idea of this project is to leverage Spark to build up ETL process song_data and log_data. Totally 5 tables are created, `song_play` is the fact table, and `songs, artists, users, time` are 4 dimensional table. 

## Project Usage

The project include 4 objects: 

- `README.md` is the readme file.
- `etl.py` preform data ETL to read and parse data from S3 JSON and write into correct format to tables, and save it back to S3 as parquet data.
- `elt_notebook.ipynb` is the notebook to draft `etl.py`.
- `df.cfg` is the config file for AWS credentials.

Run the project by runing `etl.py` to parse the data and write into the table created.
