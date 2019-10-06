# drop tables
player_history_drop = 'DROP TABLE IF EXISTS player_history;'
tourney_history_drop = 'DROP TABLE IF EXISTS tourney_history;'
matches_drop = 'DROP TABLE IF EXISTS matches;'
ranking_drop = 'DROP TABLE IF EXISTS ranking;'
tourney_drop = 'DROP TABLE IF EXISTS tourney;'
player_drop = 'DROP TABLE IF EXISTS player;'
atp_matches_log_drop = 'DROP TABLE IF EXISTS atp_matches_log;'

drop_table_queries = [player_history_drop, tourney_history_drop, matches_drop, 
                      ranking_drop, tourney_drop, player_drop, atp_matches_log_drop]

# create table
# Dimension table
tourney_create = """
    CREATE TABLE IF NOT EXISTS tourney (
        id varchar PRIMARY KEY,
        name varchar,
        surface varchar,
        level varchar,
        date date,
        year int
    );
    """

player_create = """
    CREATE TABLE IF NOT EXISTS player (
        id int PRIMARY KEY,
        first_name varchar, 
        last_name varchar,
        hand varchar(1),
        birth_year int,
        country varchar(3)
    )
"""

ranking_create = """
    CREATE TABLE IF NOT EXISTS ranking (
        ranking_date date,
        rank int,
        player_id int, 
        seed int, 
        points int,
        PRIMARY KEY (ranking_date, rank)
    )
"""

matches_create = """
CREATE TABLE IF NOT EXISTS matches (
    tourney_id varchar,
    match_id int,
    round varchar,
    minutes float,
    winner_id int,
    loser_id int,
    score_set1 varchar,
    score_set2 varchar,
    score_set3 varchar,
    score_set4 varchar,
    score_set5 varchar,
    PRIMARY KEY (tourney_id, match_id)
)
"""


# Fact Table
player_history_create = """
CREATE TABLE IF NOT EXISTS player_history (
    player_id int,
    player_first_name varchar,
    player_last_name varchar,
    tourney_id varchar,
    tourney_name varchar,
    tourney_year int,
    player_ranking int,
    player_seed int,
    player_best varchar,
    total_game_played int,
    PRIMARY KEY (player_id, tourney_id)
)
"""

tourney_champions_create = """
CREATE TABLE IF NOT EXISTS tourney_champions (
    tourney_id varchar NOT NULL,
    tourney_name varchar,
    tourney_year int,
    player_id int,
    player_first_name varchar,
    player_last_name varchar,
    player_country varchar(3),
    player_hand varchar,
    player_rank int,
    player_seed int,
    PRIMARY KEY (tourney_id, player_id)
)
"""

atp_matches_log_create = """
CREATE TABLE IF NOT EXISTS atp_matches_log (
    tourney_id varchar, 
    tourney_name varchar, 
    surface varchar, 
    draw_size varchar, 
    tourney_level varchar, 
    tourney_date varchar, 
    match_num varchar, 
    winner_id varchar, 
    winner_seed varchar, 
    winner_entry varchar, 
    winner_name varchar, 
    winner_hand varchar, 
    winner_ht varchar, 
    winner_ioc varchar, 
    winner_age varchar, 
    loser_id varchar, 
    loser_seed varchar, 
    loser_entry varchar, 
    loser_name varchar, 
    loser_hand varchar, 
    loser_ht varchar, 
    loser_ioc varchar, 
    loser_age varchar, 
    score varchar, 
    best_of varchar, 
    round varchar, 
    minutes varchar, 
    w_ace varchar, 
    w_df varchar, 
    w_svpt varchar, 
    w_1stIn varchar, 
    w_1stWon varchar, 
    w_2ndWon varchar, 
    w_SvGms varchar, 
    w_bpSaved varchar, 
    w_bpFaced varchar, 
    l_ace varchar, 
    l_df varchar, 
    l_svpt varchar, 
    l_1stIn varchar, 
    l_1stWon varchar, 
    l_2ndWon varchar, 
    l_SvGms varchar, 
    l_bpSaved varchar, 
    l_bpFaced varchar, 
    winner_rank varchar, 
    winner_rank_points varchar, 
    loser_rank varchar, 
    loser_rank_points varchar
)
"""

create_table_queries = [player_history_create, tourney_champions_create, matches_create, 
                        ranking_create, tourney_create, player_create, atp_matches_log_create]