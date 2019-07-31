import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import DoubleType, TimestampType
import pandas as pd


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('CREDENTIAL','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('CREDENTIAL','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data+'/song_data/A/B/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("song_data")
    df.write.mode('overwrite').parquet('/song_data')

    # extract columns to create songs table
    songs_table = spark.sql('''
          SELECT DISTINCT song_id, title, artist_id, year, duration FROM song_data
          ''')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year','artist_id').parquet(output_data+"/songs")

    # extract columns to create artists table
    artists_table = spark.sql('''
        SELECT DISTINCT artist_id, 
               artist_name as name, 
               artist_location as location,
               artist_latitude as latitude,
               artist_longitude as longitude
        FROM song_data
        ''')
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+"/artists")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data+'/log_data/2018/11/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where("page=='NextSong'")
    df.createOrReplaceTempView("log_data")

    # extract columns for users table    
    users_table = spark.sql('''
        SELECT DISTINCT userid as user_id, 
           firstname as first_name,
           lastname as last_name,
           gender as gender,
           level as level
        FROM log_data
    ''')
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+"/users")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x/1000, DoubleType())
    df = df.withColumn("ts_timestamp", get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: pd.datetime(1970,1,1) + pd.to_timedelta(x, unit='ms'))
    df = df.withColumn('ts_datetime', get_datetime('ts'))
    
    # extract columns to create time table
    df.createOrReplaceTempView("log_data")
    time_table = df.select(
        col('ts_timestamp').alias('start_time'),
        hour('ts_datetime').alias('hour'),
        dayofmonth('ts_datetime').alias('day'),
        weekofyear('ts_datetime').alias('week'),
        month('ts_datetime').alias('month'),
        year('ts_datetime').alias('year'), 
        date_format('ts_datetime', 'u').alias('weekday')
    )

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year','month').parquet(output_data+"/time")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+"/song_data")
    song_df.createOrReplaceTempView("song_data")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
        SELECT log.ts_timestamp as start_time,
               year(log.ts_datetime) as year,
               month(log.ts_datetime) as month,
               log.userid as user_id, 
               log.level as level,
               s.song_id as song_id,
               s.artist_id as artist_id,
               log.sessionid as session_id,
               log.location as location,
               log.useragent as user_agent
        FROM log_data log
        JOIN song_data s
        ON log.song = s.title
        AND log.artist = s.artist_name
        AND log.length = s.duration
        ''')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').mode('overwrite').parquet(output_data+"/songplays")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
