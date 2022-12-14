"""Defines the pipeline DAG, sets default args, initializes each
custom operator and specify task dependencies"""

# python libs
import os
from datetime import datetime, timedelta
# config libs
import configparser
# airflow libs
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
# plugins operators libs
from pipeline.operators.data_quality import DataQualityOperator
from pipeline.operators.load_dimension import LoadDimensionOperator
from pipeline.operators.load_fact import LoadFactOperator
from pipeline.operators.stage_redshift import StageToRedshiftOperator
# plugins helpers libs
from pipeline.helpers.sql_queries import SqlQueries

# Set config parser
parser = configparser.ConfigParser()
parser.read_file(open(os.path.abspath("./dags/pipeline.cfg"), encoding='utf-8'))

# Get S3 config
S3_BUCKET = parser.get("S3", "S3_BUCKET")
S3_LOG_DATA = parser.get("S3", "S3_LOG_DATA")
S3_SONG_DATA = parser.get("S3", "S3_SONG_DATA")

# Get DAG config
START_DATE_YEAR = int(parser.get("DAG", "START_DATE_YEAR"))
START_DATE_MONTH = int(parser.get("DAG", "START_DATE_MONTH"))
START_DATE_DAY = int(parser.get("DAG", "START_DATE_DAY"))

# Get quality check rules
QUALITY_CHECK_JSON = os.path.abspath("./dags/quality_check.json")

# Set DAG default arguments
DEFAULT_ARGS = {
    "owner": "udacity",
    "depends_on_past": False,
    "start_date": datetime(START_DATE_YEAR, START_DATE_MONTH, START_DATE_DAY),
    "schedule_interval": "@hourly",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
    "catchup": False
}

# Defines the pipeline DAG, task groups and dependencies
with DAG("pipeline_dag",
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         description="Load and transform data in Redshift with Airflow"
         ) as dag:
    start_operator = DummyOperator(task_id="begin_execution")

    with TaskGroup(group_id="stage_to_redshift") as stage_to_redshift:
        StageToRedshiftOperator(
            task_id="stage_events",
            s3_bucket=S3_BUCKET,
            s3_key=S3_LOG_DATA,
            table="staging_events",
            sql_copy=SqlQueries.table_staging_events_copy,
            sql_create=SqlQueries.table_staging_events_create,
        )

        StageToRedshiftOperator(
            task_id="stage_songs",
            s3_bucket=S3_BUCKET,
            s3_key=S3_SONG_DATA,
            table="staging_songs",
            sql_copy=SqlQueries.table_staging_songs_copy,
            sql_create=SqlQueries.table_staging_songs_create,
        )

    load_songplays_table = LoadFactOperator(
        task_id="load_songplays_fact_table",
        table="songplays",
        sql_create=SqlQueries.table_songplays_create,
        sql_insert=SqlQueries.table_songplays_insert,
    )

    with TaskGroup(group_id="load_dimension_tables") as load_dimension_tables:
        LoadDimensionOperator(
            task_id="load_user_dim_table",
            table="users",
            sql_create=SqlQueries.table_users_create,
            sql_insert=SqlQueries.table_users_insert,
        )

        LoadDimensionOperator(
            task_id="load_song_dim_table",
            table="songs",
            sql_create=SqlQueries.table_songs_create,
            sql_insert=SqlQueries.table_songs_insert,
        )

        LoadDimensionOperator(
            task_id="load_artist_dim_table",
            table="artists",
            sql_create=SqlQueries.table_artists_create,
            sql_insert=SqlQueries.table_artists_insert,
        )

        LoadDimensionOperator(
            task_id="load_time_dim_table",
            table="time",
            sql_create=SqlQueries.table_time_create,
            sql_insert=SqlQueries.table_time_insert,
        )

    run_quality_checks = DataQualityOperator(
        task_id="run_data_quality_checks",
        rules=QUALITY_CHECK_JSON
    )

    stop_operator = DummyOperator(task_id="stop_execution")

    start_operator >> stage_to_redshift >> load_songplays_table
    load_songplays_table >> load_dimension_tables >> run_quality_checks >> stop_operator
