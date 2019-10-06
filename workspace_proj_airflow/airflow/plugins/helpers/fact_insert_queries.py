class FactInsertQueries:
    player_history_insert = ("""
    INSERT INTO player_history
    SELECT m.player_id, p.first_name as player_first_name, p.last_name as player_last_name,
           m.tourney_id, t.name, t.year, r.rank, r.seed, 
           CASE WHEN m.champion_flag = 1 THEN 'CHAMPION' ELSE matches.round END as player_best,
            m.total_game as total_game_played
    FROM
        (SELECT re.tourney_id, re.player_id, max(re.best_match_id) as best_match_id, sum(re.total_game) as total_game, 
               max(re.champion_flag) as champion_flag
        FROM (
            SELECT tourney_id, loser_id as player_id, max(match_id) as best_match_id, count(match_id) as total_game, 
            0 as champion_flag
            FROM matches
            GROUP BY tourney_id, loser_id
            UNION 
            SELECT tourney_id, winner_id as player_id, match_id as best_match_id, 1 as total_game, 1 as champion_flag
            FROM matches
            WHERE round = 'F'
        ) re
        GROUP BY re.tourney_id, re.player_id) m
    JOIN matches 
    on m.tourney_id = matches.tourney_id
    and m.best_match_id = matches.match_id
    JOIN player p
    ON m.player_id = p.id
    JOIN tourney t
    ON m.tourney_id = t.id
    JOIN ranking r
    ON t.date = r.ranking_date
    and m.player_id = r.player_id;
    """)

    tourney_champions_insert = ("""
    INSERT INTO tourney_champions
    SELECT t.id as tourney_id, t.name as tourney_name, t.year as tourney_year,
           m.winner_id as player_id, p.first_name as player_first_name, p.last_name as player_last_name,
           p.country as player_country, p.hand as player_hand, 
            r.rank as player_rank, r.seed as player_seed
    FROM tourney t, matches m, player p, ranking r
    WHERE t.id = m.tourney_id
    AND p.id = m.winner_id
    AND r.ranking_date = t.date
    and r.player_id = m.winner_id
    AND m.round = 'F';
    """)
