# CREATE TABLES
artists_create = ("""
CREATE TABLE public.artists (artistid varchar(256) NOT NULL, name varchar(256), location varchar(256), 
lattitude numeric(18,0),longitude numeric(18,0));
""")

songplays_create = ("""
CREATE TABLE public.songplays (
songplay_id varchar(32) NOT NULL,
start_time timestamp NOT NULL,
userid int4 NOT NULL,
"level" varchar(256),
songid varchar(256),
artistid varchar(256),
sessionid int4,
location varchar(256),
user_agent varchar(256),
CONSTRAINT songplays_pkey PRIMARY KEY (songplay_id));
""")

songs_create = ("""
CREATE TABLE public.songs (
songid varchar(256) NOT NULL,
title varchar(256),
artistid varchar(256),
"year" int4,
duration numeric(18,0),
CONSTRAINT songs_pkey PRIMARY KEY (songid));
""")

staging_events_create = ("""
CREATE TABLE public.staging_events (
artist varchar(256),
auth varchar(256),
firstName varchar(256),
gender varchar(256),
itemInSession int4,
lastName varchar(256),
length numeric(18,0),
"level" varchar(256),
location varchar(256),
"method" varchar(256),
page varchar(256),
registration numeric(18,0),
sessionId int4,
song varchar(256),
status int4,
ts int8,
userAgent varchar(256),
userId int4);
""")

staging_songs_create = ("""
CREATE TABLE public.staging_songs (
num_songs int4,
artist_id varchar(256),
artist_name varchar(256),
artist_latitude numeric(18,0),
artist_longitude numeric(18,0),
artist_location varchar(256),
song_id varchar(256),
title varchar(256),
duration numeric(18,0),
"year" int4);
""")

users_create = ("""
CREATE TABLE public.users (
userid int4 NOT NULL,
first_name varchar(256),
last_name varchar(256),
gender varchar(256),
"level" varchar(256),
CONSTRAINT users_pkey PRIMARY KEY (userid));
""")

time_create = ("""
CREATE TABLE time (
    start_time varchar(20) PRIMARY KEY,
    hour varchar,
    day varchar,
    week varchar,
    month varchar,
    year varchar,
    weekday varchar);
""")

# QUERY LISTS
create_table_queries = [songplays_create, artists_create, songs_create, staging_events_create, staging_songs_create, users_create, time_create]