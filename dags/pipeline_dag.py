from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy import DummyOperator

from operators import DataQualityOperator
from operators import LoadDimensionOperator
from operators import LoadFactOperator
from operators import StageToRedshiftOperator
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
}

with DAG('pipeline_dag',
         default_args=default_args,
         description='Load and transform data in Redshift with Airflow',
         schedule_interval='0 * * * *'
         ) as dag:

    start_operator = DummyOperator(task_id='begin_execution', dag=dag)

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='stage_events'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='stage_songs'
    )

    load_songplays_table = LoadFactOperator(
        task_id='load_songplays_fact_table'
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='load_user_dim_table'
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='load_song_dim_table'
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='load_artist_dim_table'
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='load_time_dim_table'
    )

    run_quality_checks = DataQualityOperator(
        task_id='run_data_quality_checks'
    )

    stop_operator = DummyOperator(task_id='stop_execution')

    stage_to_redshift = [stage_events_to_redshift, stage_songs_to_redshift]

    load_dimension_tables = [
        load_song_dimension_table,
        load_user_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table
    ]

    start_operator >> stage_to_redshift >> load_songplays_table
    load_songplays_table >> load_dimension_tables >> run_quality_checks >> stop_operator