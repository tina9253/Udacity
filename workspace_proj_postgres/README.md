## Project Summary

The purpose of this database is to create a data structure for Sparkify to do analytical summaries for the user activities. Relative usage could include: total active users on given day, or total user-active minutes trend in Nov 2018 etc. 

The overall design of this database follows star schema with 1 fact table and 4 dimensional table, which indicates songs, artists, users, and time. Star schema is leveraged since each dimension table is stright-forward and there are not duplicated info in each table if given what we defined. So totally 5 tables are created, `song_play` is the fact table, and `songs, artists, users, time` are 4 dimensional table. 

Each table has 1 primary key, so that we won't have any duplicates in each table. And `songplay_id` is a serial primary key where it will auto increment as we insert new values, since basically it's a log file and it's records should not be replicated.

## Project Usage

The project include 8 objects: 

- `README.md` is the readme file.
- `sql_queries.py` includes all SQL query that are used in this projects, indlucding create table, drop table, insert values, etc.
- `create_tables.py` calls queries in `sql_queries.py` to generate tables so that later we can insert values into it. 
- `etl.py` preform data ETL to read and parse data from JSON and write into correct format to tables.
- `elt.ipynb` is the notebook to draft `etl.py`.
- `test.ipynb` is the notebook to check whether desired tables are populated. 
- `DataQualityCheck.ipynb` is to use Python pandas to check whether the results in database is the same. As I noticed that in this project, there is only one matched song that appears in both songs data and log data. 
- `data` is the folder including sample data for both song info and song play log data.

Run the project by following the steps:

1. First, you need to run the `create_tables.py` to create tables if they do not exist. If they exists however, it will drop all existing tables and create new ones.
2. Second, run `etl.py` to parse the data and write into the table created.
3. You can use `test.ipynb` to check the results. 

## Analytical Example

Some simple analytical example of this database are asking the following questions:

#### a. Which songs are the top 10 most played? 

Query: 

`SELECT a.song_id, s.title, count(*) <br>
 FROM songplays a, songs s 
 WHERE a.song_id = s.song_id 
 GROUP BY a.song_id, s.title;`
 
#### b. Which users are the most active ones who plays the most songs per day? 

Query:
 
 `SELECT a.user_id, u.first_name, u.last_name, t.year, t.month, t.day, count(*) 
  FROM songplays a, users u, time t 
  WHERE a.user_id = u.user_id 
  AND a.start_time = t.start_time 
  GROUP BY a.user_id, u.first_name, u.last_name, t.year, t.month, t.day 
  ORDER BY t.year, t.month, t.day, count(*) DESC`
  
If you run the query, you can see that given the log, for example, on Nov 2rd 2018, Lily Koch is the most active user and she totally listened for 74 songs.