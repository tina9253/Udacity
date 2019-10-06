from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (RawToPostgresOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import FactInsertQueries

default_args = {
    'owner': 'tinaliu',
    'start_date': datetime(2019, 10, 2),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'email_on_retry': False
}

dag = DAG('tennis_atp_dag',
          default_args=default_args,
          description='Load and transform history tennis ATP data',
          schedule_interval='0 * * * *',
          catchup=True
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_matches_to_postgres = RawToPostgresOperator(
    task_id='stage_matches',
    dag=dag,
    conn_id="tennis_atp",
    table="atp_matches_log",
    file_dir=os.getcwd() + '/tennis_atp',
    file_pattern='atp_matches_[0-9]*.csv'
)

stage_qual_chall_to_postgres = RawToPostgresOperator(
    task_id='stage_qual_chall',
    dag=dag,
    conn_id="tennis_atp",
    table="atp_matches_log",
    file_dir=os.getcwd() + '/tennis_atp',
    file_pattern='atp_matches_qual_chall_.*.csv'
)

stage_futures_to_postgres = RawToPostgresOperator(
    task_id='stage_futures',
    dag=dag,
    conn_id="tennis_atp",
    table="atp_matches_log",
    file_dir=os.getcwd() + '/tennis_atp',
    file_pattern='atp_matches_futures_.*.csv'
)

# load dimension table

load_player_dimension_table = LoadDimensionOperator(
    task_id='Load_player_table',
    dag=dag,
    conn_id='tennis_atp',
    table='player'
)

load_matches_dimension_table = LoadDimensionOperator(
    task_id='Load_matches_table',
    dag=dag,
    conn_id='tennis_atp',
    table='matches'
)

load_tourney_dimension_table = LoadDimensionOperator(
    task_id='Load_tourney_table',
    dag=dag,
    conn_id='tennis_atp',
    table='tourney'
)

load_ranking_dimension_table = LoadDimensionOperator(
    task_id='Load_ranking_table',
    dag=dag,
    conn_id='tennis_atp',
    table='ranking'
)

# load fact table
load_player_history_table = LoadFactOperator(
    task_id='Load_player_history_table',
    dag=dag,
    conn_id='tennis_atp',
    table='player_history',
    insert_query=FactInsertQueries.player_history_insert
)

load_tourney_champions_table = LoadFactOperator(
    task_id='Load_tourney_champions_table',
    dag=dag,
    conn_id='tennis_atp',
    table='tourney_champions',
    insert_query=FactInsertQueries.tourney_champions_insert
)

# data quality
run_player_dq_check = DataQualityOperator(
    task_id='Player_dq_checks',
    dag=dag,
    table='player',
    conn_id='tennis_atp'
)

run_tourney_dq_check = DataQualityOperator(
    task_id='Tourney_dq_checks',
    dag=dag,
    table='tourney',
    conn_id='tennis_atp'
)

run_ranking_dq_check = DataQualityOperator(
    task_id='Ranking_dq_checks',
    dag=dag,
    table='ranking',
    conn_id='tennis_atp'
)

run_matches_dq_check = DataQualityOperator(
    task_id='Matches_dq_checks',
    dag=dag,
    table='matches',
    conn_id='tennis_atp'
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_matches_to_postgres
start_operator >> stage_futures_to_postgres
start_operator >> stage_qual_chall_to_postgres
stage_matches_to_postgres >> load_player_dimension_table
stage_futures_to_postgres >> load_player_dimension_table
stage_qual_chall_to_postgres >> load_player_dimension_table
stage_matches_to_postgres >> load_tourney_dimension_table
stage_futures_to_postgres >> load_tourney_dimension_table
stage_qual_chall_to_postgres >> load_tourney_dimension_table
stage_matches_to_postgres >> load_ranking_dimension_table
stage_futures_to_postgres >> load_ranking_dimension_table
stage_qual_chall_to_postgres >> load_ranking_dimension_table
stage_matches_to_postgres >> load_matches_dimension_table
stage_futures_to_postgres >> load_matches_dimension_table
stage_qual_chall_to_postgres >> load_matches_dimension_table
load_player_dimension_table >> run_player_dq_check
load_tourney_dimension_table >> run_tourney_dq_check
load_ranking_dimension_table >> run_ranking_dq_check
load_matches_dimension_table >> run_matches_dq_check
run_player_dq_check >> load_player_history_table
run_tourney_dq_check >> load_player_history_table
run_ranking_dq_check >> load_player_history_table
run_matches_dq_check >> load_player_history_table
run_player_dq_check >> load_tourney_champions_table
run_tourney_dq_check >> load_tourney_champions_table
run_ranking_dq_check >> load_tourney_champions_table
run_matches_dq_check >> load_tourney_champions_table
load_player_history_table >> end_operator
load_tourney_champions_table >> end_operator
