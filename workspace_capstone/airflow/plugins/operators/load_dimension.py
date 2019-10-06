from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table,
                 conn_id="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.conn_id = conn_id
        self.dim_mapping = {'player': self.get_player_info,
                            'ranking': self.get_ranking_info,
                            'tourney': self.get_tourney_info,
                            'matches': self.get_matches_info}
    
    def insert_update_pd(self, row, table_name, schema, conflict_key):
        values = tuple(row.values)
        update_str = ', '.join([f"{k} = {v}" for k, v in row.to_dict().items()])
        insert_update_query = f""" 
        INSERT INTO {table_name} ({schema})
        VALUES {values}
        ON CONFLICT ({conflict_key})
        DO NOTHING ;
        """
        insert_update_query = insert_update_query.replace(', None', ', NULL')
        return insert_update_query

    def get_player_info(self, matches: pd.DataFrame):
        winner_col = ['tourney_date', 'winner_id', 'winner_name', 'winner_ioc', 'winner_hand', 'winner_age']
        loser_col = ['tourney_date', 'loser_id', 'loser_name', 'loser_ioc', 'loser_hand', 'loser_age']
        to_col = ['tourney_date', 'id', 'name', 'country', 'hand', 'age']
        schema = ['id', 'first_name', 'last_name', 'hand', 'birth_year', 'country']

        winner = matches[winner_col]
        winner.columns = to_col
        loser = matches[loser_col]
        loser.columns = to_col

        player = pd.concat([winner, loser]).drop_duplicates()
        player['age'] = player['age'].astype(float)
        player['first_name'] = player['name'].apply(lambda x: x.split(" ")[0])
        player['last_name'] = player['name'].apply(lambda x: x.split(" ")[len(x.split(" "))-1])
        player['birth_year'] = (pd.to_datetime(player['tourney_date'], format='%Y%m%d') 
                                - pd.to_timedelta(player['age'], unit='y')).dt.year
        return player[schema], ['id']
    
    def get_tourney_info(self, matches: pd.DataFrame):
        from_col = ['tourney_id', 'tourney_name', 'tourney_date', 'surface', 'tourney_level']
        to_col = ['id', 'name', 'date', 'surface', 'level']
        schema = ['id', 'name', 'surface', 'level', 'date', 'year']

        tourney = matches[from_col].drop_duplicates()
        tourney.columns = to_col
        tourney['year'] = pd.to_datetime(tourney['date'], format='%Y%m%d').dt.year
        tourney['date'] = pd.to_datetime(tourney['date'], format='%Y%m%d').dt.date.astype(str)
        return tourney[schema], ['id']
    
    def get_ranking_info(self, matches: pd.DataFrame):
        winner_col = ['tourney_date', 'winner_id', 'winner_seed', 'winner_rank', 'winner_rank_points']
        loser_col = ['tourney_date', 'loser_id', 'loser_seed', 'loser_rank', 'loser_rank_points']
        to_col = ['ranking_date', 'player_id', 'seed', 'rank', 'points']
        schema = ['ranking_date', 'rank', 'player_id', 'seed', 'points']

        winner = matches[winner_col]
        winner.columns = to_col
        loser = matches[loser_col]
        loser.columns = to_col

        ranking = pd.concat([winner, loser]).drop_duplicates()
        ranking['ranking_date'] = pd.to_datetime(ranking['ranking_date'], format='%Y%m%d').dt.date.astype(str)

        return ranking[schema], ['ranking_date', 'rank']
    
    def get_matches_info(self, matches):
        from_col = ['tourney_id', 'match_num', 'round', 'minutes', 'winner_id', 'loser_id', 'score']
        schema = ['tourney_id', 'match_id', 'round', 'minutes', 'winner_id', 'loser_id', 'score']

        matches = matches[from_col]
        matches.columns = schema

        set_score = matches['score'].str.split(" ", n=4, expand=True)
        set_num = set_score.shape[1]
        set_cols = [f'score_set{i}' for i in range(1, set_num+1)]

        matches[set_cols] = set_score
        return matches.drop(['score'], axis=1), ['tourney_id', 'match_id']
    
    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        df = hook.get_pandas_df(sql='select * from atp_matches_log;')
        df_load, key = self.dim_mapping[self.table](df)
        prim_key = ', '.join(key)
        print(df.shape)
        print(df.columns)
        df_load = df_load.where((pd.notnull(df_load)), None)
        schema = ','.join(df_load.columns)
        for i, row in df_load.iterrows():
            insert_update_query = self.insert_update_pd(row, self.table, schema, prim_key)
            try:
                hook.run(insert_update_query)
            except Exception as e:
                print(e)
        self.log.info(f"{self.table} loaded successfully")


