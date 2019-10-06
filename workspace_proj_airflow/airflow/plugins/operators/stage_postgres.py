from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
import os
import re

class RawToPostgresOperator(BaseOperator):
    ui_color = '#358140'
    
    @apply_defaults
    def __init__(self,
                 conn_id="",
                 table="",
                 file_dir="",
                 file_pattern="",
                 *args, **kwargs):

        super(RawToPostgresOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.postgres_conn_id = conn_id
        self.file_dir = file_dir
        self.file_pattern = file_pattern

    def execute(self, context):
        db_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        self.log.info("Loading data to Postgres")
        
        copy_query = "COPY {} FROM '{}' DELIMITERS ',' CSV HEADER;"

        if self.file_pattern != "":
            ptn_csv = re.compile(self.file_pattern)
            csv_list = list(filter(ptn_csv.search, os.listdir(self.file_dir)))
            for f in csv_list:
                print(f'Copying {f}')
                path = os.path.join(self.file_dir, f)
                db_hook.run(copy_query.format(self.table, path))
        else:
            db_hook.run(copy_query.format(self.table, self.file_dir))
            
                            
    





