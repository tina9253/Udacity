# Capstone Project - ATP Tennis Historical Tracker

## Scope
This project aims to generate a effective and well-formatted database for keeping historical records 
of Tennis ATPs competitions, players, and matches. The idea comes from the fact that I am tennis fan and
always wanted to track results and player history, to see, and predict the performance of future games. 

Basically, the project takes data of matches results in every single match in every ATP tournament. 
Data source includes 3 types of data: tournament main matches data, qualification challenge matches data, as well as 
futures matches data. 

From matches data, we can extract lots of info, and I separate them into multiple dimension table due to the 
redundancies in original data. Then I perform a few data quality check for each dimension table. After that, I perform
two queries to generate two fact tables. One is used to track for each player, what are their history of ranking, 
points, best ranking in each tournament. The other one is to track for each tourney, every year, who is the champion. 

By doing that, it could answer questions like:

- Who wins championship most frequently in 1990s? 
- Along the history, which country's player wins the most championship?
- For Roger Federer, which year he has won most championship?
- On average, how many matches to win to ready top 10 ranking?

Ideally, the data pipeline should be triggered on-demand, whenever there are tournament and everyday after the matches
finishes.

## Summary
The overall design of this database follows star schema with 2 fact table, which is used to view game 
results in different view: historical champions and players' records, and 4 dimensional table, 
which indicates player, tourney, ranking, and matches. Star schema is leveraged since each 
dimension table is straight-forward and there are not duplicated info in each table if given 
what we defined. So totally 6 tables are created, `player_history` and `tourney_champions` 
is the fact table, and `player, tourney, ranking, matches` are 4 dimensional table. Each table has primary keys. 

To enable streaming features, assuming every day there are new matches results coming in, I leveraged airflow pipeline
and build up a DAG which will execute every morning 7:05, allowing 5 min buffering for data to transfer.

If data size increases by 100X, the best solution is to trigger the pipeline by batches, and/or leverage cloud computing
, instead of airflow locally. 

If the data is needed to be access by a lot of people, I would have published the output of pipeline somewhere online, 
either in a AWS S3 bucket, or published using a visualization tool, enable visualization to help users to view the data.

## Analytical Usage Example

- Who wins Grand Slam championship most frequently in 1990s, 2000s, 2010s respectively?

```sql
SELECT t."level", round(tc.tourney_year/10)*10 as decade, tc.player_first_name, tc.player_last_name, count(*)
FROM tourney_champions tc
JOIN tourney t
ON tc.tourney_id = t.id
WHERE t."level" = 'G'
GROUP BY t."level", round(tourney_year/10)*10, player_first_name, player_last_name
ORDER BY decade;
```

- For Roger Federer, how many championship has he won between 2000 to 2010?
```sql
SELECT player_first_name, player_last_name, tourney_year, count(*)
FROM player_history
WHERE player_first_name = 'Roger'
AND player_last_name = 'Federer'
AND player_best = 'CHAMPION'
AND tourney_year > 2000
AND tourney_year <= 2010
GROUP BY tourney_year, player_first_name, player_last_name;
```


## Reference
https://github.com/JeffSackmann/tennis_atp 