from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 table="",
                 insert_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.conn_id = conn_id
        self.insert_query = insert_query
    
    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        hook.run("DELETE FROM {};".format(self.table))
        self.log.info("Loading fact table {}".format(self.table))
        hook.run(self.insert_query)
