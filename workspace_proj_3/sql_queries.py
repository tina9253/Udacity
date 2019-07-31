import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist varchar,
    auth varchar,
    firstname varchar,
    gender varchar(2),
    itemInSession integer,
    lastName varchar,
    length float,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration numeric,
    sessionId integer,
    song varchar,
    status integer,
    ts numeric,
    userAgent varchar,
    userId varchar
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs integer,
    artist_id varchar,
    artist_latitude float,
    artist_longitude float,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration float,
    year integer
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id integer IDENTITY(0,1) PRIMARY KEY, 
    start_time timestamp NOT NULL,
    user_id varchar NOT NULL,
    level varchar,
    song_id varchar NOT NULL,
    artist_id varchar NOT NULL,
    session_id integer,
    location varchar,
    user_agent varchar);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id varchar PRIMARY KEY,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY,
    title varchar,
    artist_id varchar NOT NULL,
    year integer,
    duration numeric);
""")

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY,
    name varchar,
    location varchar,
    lattitude numeric,
    longitude numeric);
"""

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY,
    hour integer,
    day integer,
    week integer,
    month integer,
    year integer,
    weekday integer);
""")

# STAGING TABLES
staging_events_copy = ("""
copy staging_events from {}
    credentials 'aws_iam_role={}'
    format as json {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
copy staging_songs from {} 
     credentials 'aws_iam_role={}'
     format as json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' as start_time,
       e.userid as user_id, 
       e.level as level,
       s.song_id as song_id,
       s.artist_id as artist_id,
       e.sessionid as session_id,
       e.location as location,
       e.useragent as user_agent
FROM staging_events e
JOIN staging_songs s
ON e.song = s.title
AND e.artist = s.artist_name
AND e.length = s.duration
WHERE e.page = 'NextSong'
;
""")

user_table_insert = ("""
INSERT INTO users
SELECT DISTINCT e.userid as user_id, 
       e.firstname as first_name,
       e.lastname as last_name,
       e.gender as gender,
       e.level as level
FROM staging_events e
WHERE e.page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs
SELECT DISTINCT song_id, title, artist_id, year, duration 
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists
SELECT DISTINCT artist_id, 
       artist_name as name, 
       artist_location as location,
       artist_latitude as latitude,
       artist_longitude as longitude
FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time
SELECT DISTINCT start_time, 
       DATE_PART(h, start_time) as hour,
       DATE_PART(doy, start_time) as day,
       DATE_PART(w, start_time) as week,
       DATE_PART(mon, start_time) as month,
       DATE_PART(y, start_time) as year,
       DATE_PART(dow, start_time) as weekday
FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
