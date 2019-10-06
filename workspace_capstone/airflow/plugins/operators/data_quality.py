from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 table,
                 conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table_not_null = {'player': ['id','first_name','last_name'],
                          'tourney': ['id', 'name', 'date'],
                          'ranking': ['ranking_date', 'rank', 'player_id'],
                          'matches': ['tourney_id', 'match_id', 'round']}
        self.table = table

    def _check_null(self, hook, table, col):
        query = f'SELECT count(*) FROM {table} WHERE {col} IS NULL'
        records = hook.get_records(query)
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {table} returned no results")
        num_records = records[0][0]
        if num_records > 0:
            raise ValueError(f"Data quality check failed. {table}.{col} contained {num_records} not null rows")
        self.log.info(f"Data quality on table {table}.{col} check passed for not null columns")
        
    def _check_greater_than_zero(self, hook, table):
        query = f'SELECT count(*) FROM {table}'
        records = hook.get_records(query)
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {table} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {table} contained 0 rows")
        self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")

    def execute(self, context):
        
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        
        self.log.info('Checking Not Null Columns')
        for col in self.table_not_null[self.table]:
            self._check_null(hook, self.table, col)
        
        self.log.info('Checking Existing Row')
        self._check_greater_than_zero(hook, self.table)
