# analytics_daily_etl.py
"""
Analytics Daily ETL DAG

This DAG extracts, validates and loads daily analytics data with quality checks
from operational database to analytics warehouse.
"""
from datetime import datetime, timedelta
from airflow.sdk import dag, task
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
import itertools
import pandas as pd
import numpy as np
import logging
from textwrap import dedent
from analytics_daily_etl_queries import (
    QUERY_FACT_DAILY_FEED_EXTRACT
    , QUERY_FACT_DAILY_POSTS_EXTRACT
    , QUERY_FACT_DAILY_MESSENGER_EXTRACT
    , QUERY_FACT_USER_CONNECTIONS_EXTRACT
    , QUERY_DIM_USERS_EXTRACT
    , QUERY_FACT_DAILY_FEED_LOAD
    , QUERY_FACT_DAILY_POSTS_LOAD
    , QUERY_FACT_DAILY_MESSENGER_LOAD
    , QUERY_FACT_USER_CONNECTIONS_LOAD
    , QUERY_DIM_USERS_LOAD    
)

# Database connections
SRC_DB_ID = 'ch_src'
DST_DB_ID = 'ch_dst'
src_hook = ClickHouseHook(clickhouse_conn_id=SRC_DB_ID)
dst_hook = ClickHouseHook(clickhouse_conn_id=DST_DB_ID)
logger = logging.getLogger(__name__)

# ==========================================================================
# DAG config
# ==========================================================================

default_args = {
    'owner': 'analytics_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 25),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
}

dag_config = {
    'default_args': default_args,
    'description': 'Daily ETL pipeline for loading data from operational DB to analytics warehouse',
    'schedule': '0 3 * * *', # Runs at 3 AM daily
    'catchup': False,
    'tags': ['analytics', 'etl'],
    'max_active_runs': 1,
    'doc_md': dedent("""
    # Analytics Daily ETL with Data Validation
    
    Extracts, validates and loads daily analytics data with quality checks.
    """)
}

# ==========================================================================
# Constants for validation
# ==========================================================================

REQUIRED_COLUMNS = {
    'daily_feed': [
        'date', 'user_id', 'first_view_time', 'first_like_time', 
        'last_view_time', 'last_like_time', 'posts', 'views', 'likes',
        'avg_time_between_views', 'avg_time_between_likes', 'avg_view_to_like_seconds'
    ],
    'daily_posts': [
        'date', 'post_id', 'first_action_time', 'last_action_time', 
        'views', 'likes', 'unique_viewers', 'unique_likers',
        'avg_time_between_views', 'avg_time_between_likes', 'avg_view_to_like_seconds'
    ],
    'daily_messenger': [
        'date', 'user_id', 'first_sent_time', 'last_sent_time', 'users_sent', 
        'messages_sent', 'avg_time_between_messages_sent', 'first_received_time', 
        'last_received_time', 'users_received', 'messages_received', 
        'avg_time_between_messages_received'
    ],
    'user_connections': ['date', 'sender_id', 'receiver_id', 'messages_count'],
    'users': [
        'user_id', 'gender', 'age', 'source', 'os', 'city', 
        'country', 'exp_group', 'version'
    ]
}

# ==========================================================================
# Helper functions
# ==========================================================================

def extract_data(query: str, data_type: str) -> pd.DataFrame:
    """Common extraction logic"""
    try:
        logger.info(f"🚀 Starting {data_type} extraction...")
        records, column_types = src_hook.execute(query, with_column_types=True)
        columns = [col[0] for col in column_types]
        df = pd.DataFrame(records, columns=columns)
        logger.info(f"✅ Successfully extracted {len(df)} {data_type} records")
        logger.info(f"DataFrame shape: {df.shape}, columns: {list(df.columns)}")
        return df
    except Exception as e:
        logger.error(f"❌ Failed to extract {data_type} data: {str(e)}")
        raise

def validate_data(df: pd.DataFrame, data_type: str) -> None:
    """Common validation logic"""
    try:
        logger.info(f"🔍 Validating {data_type} data...")
        
        if df.empty:
            raise ValueError(f"❌ {data_type.capitalize()} DataFrame is empty")
        
        required_columns = REQUIRED_COLUMNS[data_type]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"❌ Missing required columns in {data_type}: {missing_columns}")
        
        logger.info(f"✅ {data_type.capitalize()} validation passed: {len(df)} records")
        
    except Exception as e:
        logger.error(f"❌ {data_type.capitalize()} validation failed: {str(e)}")
        raise

def load_data(df: pd.DataFrame, load_query: str, data_type: str) -> None:
    """Common loading logic"""
    try:
        logger.info(f"📥 Loading {len(df)} {data_type} records...")
        dst_hook.execute(load_query, df.values.tolist())  
        logger.info(f"✅ Successfully loaded {len(df)} {data_type} records")
    except Exception as e:
        logger.error(f"❌ Failed to load {data_type} records: {str(e)}")
        raise

def handle_etl_failure(context):
    """Enhanced error handling for ETL tasks"""
    task_instance = context['task_instance']
    exception = context.get('exception')
    execution_date = context['execution_date']

    logger.error(f"ETL Task {task_instance.task_id} failed on {execution_date}")
    logger.error(f"Exception: {str(exception)}")
    logger.error(f"Task try number: {task_instance.try_number}")

    # Send email notification (uses Airflow's default email config)
    try:
        task_instance.email_on_failure(subject=f"ETL Failure: {task_instance.task_id}", html_content=None)
    except Exception as e:
        logger.error(f"Failed to send failure email: {e}")
        
@dag(**dag_config)
def analytics_daily_etl():
    # ==========================================================================
    # Extract Tasks
    # ==========================================================================
    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_etl_failure
    )
    def extract_daily_feed() -> pd.DataFrame:
        """Extracts daily feed data"""
        return extract_data(QUERY_FACT_DAILY_FEED_EXTRACT, 'daily_feed')
    
    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_etl_failure
    )
    def extract_daily_posts() -> pd.DataFrame:
        """Extracts daily posts data"""
        return extract_data(QUERY_FACT_DAILY_POSTS_EXTRACT, 'daily_posts')
    
    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_etl_failure
    )
    def extract_daily_messenger() -> pd.DataFrame:
        """Extracts daily messenger data"""
        return extract_data(QUERY_FACT_DAILY_MESSENGER_EXTRACT, 'daily_messenger')
    
    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_etl_failure
    )
    def extract_user_connections() -> pd.DataFrame:
        """Extracts user connection data"""
        return extract_data(QUERY_FACT_USER_CONNECTIONS_EXTRACT, 'user_connections')
    
    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_etl_failure
    )
    def extract_users() -> pd.DataFrame:
        """Extracts users data"""
        return extract_data(QUERY_DIM_USERS_EXTRACT, 'users')

    # ==========================================================================
    # Validate Tasks
    # ==========================================================================
    @task(
        retries=2,
        retry_delay=timedelta(minutes=2),
        on_failure_callback=handle_etl_failure
    )
    def validate_daily_feed(df_daily_feed: pd.DataFrame) -> bool:
        """Validates daily feed data quality"""
        return validate_data(df_daily_feed, 'daily_feed')

    @task(
        retries=2,
        retry_delay=timedelta(minutes=2),
        on_failure_callback=handle_etl_failure
    )
    def validate_daily_posts(df_daily_posts: pd.DataFrame) -> bool:
        """Validates daily posts data quality"""
        return validate_data(df_daily_posts, 'daily_posts')

    @task(
        retries=2,
        retry_delay=timedelta(minutes=2),
        on_failure_callback=handle_etl_failure
    )
    def validate_daily_messenger(df_daily_messenger: pd.DataFrame) -> bool:
        """Validates daily messenger data quality"""
        return validate_data(df_daily_messenger, 'daily_messenger')

    @task(
        retries=2,
        retry_delay=timedelta(minutes=2),
        on_failure_callback=handle_etl_failure
    )
    def validate_user_connections(df_user_connections: pd.DataFrame) -> bool:
        """Validates user connections data quality"""
        return validate_data(df_user_connections, 'user_connections')

    @task(
        retries=2,
        retry_delay=timedelta(minutes=2),
        on_failure_callback=handle_etl_failure
    )
    def validate_users(df_users: pd.DataFrame) -> bool:
        """Validates users dimension data quality"""
        return validate_data(df_users, 'users')
        
    # ==========================================================================
    # Load Tasks
    # ==========================================================================
    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_etl_failure
    )
    def load_daily_feed(df_daily_feed: pd.DataFrame) -> None:
        """Loads daily feed data"""
        load_data(df_daily_feed, QUERY_FACT_DAILY_FEED_LOAD, 'daily_feed')
    
    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_etl_failure
    )
    def load_daily_posts(df_daily_posts: pd.DataFrame) -> None:
        """Loads daily posts data"""
        load_data(df_daily_posts, QUERY_FACT_DAILY_POSTS_LOAD, 'daily_posts')
   
    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_etl_failure
    )
    def load_daily_messenger(df_daily_messenger: pd.DataFrame) -> None:
        """Loads daily messenger data"""
        load_data(df_daily_messenger, QUERY_FACT_DAILY_MESSENGER_LOAD, 'daily_messenger')
        
    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_etl_failure
    )
    def load_user_connections(df_user_connections: pd.DataFrame) -> None:
        """Loads user connections data"""
        load_data(df_user_connections, QUERY_FACT_USER_CONNECTIONS_LOAD, 'user_connections')
        
    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_etl_failure
    )
    def load_users(df_users: pd.DataFrame) -> None:
        """Loads users data"""
        load_data(df_users, QUERY_DIM_USERS_LOAD, 'users')
                                       
    # ==========================================================================
    # WORKFLOW
    # ==========================================================================
    # Extract
    df_daily_feed = extract_daily_feed()
    df_daily_posts = extract_daily_posts()
    df_daily_messenger = extract_daily_messenger()
    df_user_connections = extract_user_connections()
    df_users = extract_users()

    # Validate
    feed_valid = validate_daily_feed(df_daily_feed)
    posts_valid = validate_daily_posts(df_daily_posts)
    messenger_valid = validate_daily_messenger(df_daily_messenger)
    connections_valid = validate_user_connections(df_user_connections)
    users_valid = validate_users(df_users)

    # Load facts
    load_daily_feed_task = load_daily_feed(df_daily_feed)
    load_daily_posts_task = load_daily_posts(df_daily_posts)
    load_daily_messenger_task = load_daily_messenger(df_daily_messenger)
    load_user_connections_task = load_user_connections(df_user_connections)
    
    # Load dimensions
    load_users_task = load_users(df_users)

    # Tasks dependences
    feed_valid >> load_daily_feed_task
    posts_valid >> load_daily_posts_task
    messenger_valid >> load_daily_messenger_task
    connections_valid >> load_user_connections_task
    users_valid >> load_users_task
    
# ==========================================================================
# Instantiate the DAG
# ==========================================================================
analytics_daily_etl()