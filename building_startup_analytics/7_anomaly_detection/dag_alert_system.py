from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from textwrap import dedent
from dotenv import load_dotenv
import sys
import os
import logging
current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, '.env')
sys.path.insert(0, current_dir)
from utils_for_dags import (
    ChConnector
    , TelegramBot
    , create_anomaly_chart
    , calculate_anomaly_bounds_mad
)
load_dotenv(env_path)
CHAT_ID_ALERT = os.getenv('CHAT_ID_ALERT')
db = ChConnector()
bot = TelegramBot(chat_id=CHAT_ID_ALERT)

# Configure logger
logger = logging.getLogger('alert_system')
logger.setLevel(logging.INFO)

default_args = {
    'owner': 'Pavel Grigoryev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 9, 25),
}

dag_config = {
    'default_args': default_args,
    'description': 'DAG for detecting anomalies and sending report in Telegram',
    'schedule_interval': '*/15 * * * *', # Every 15 minutes
    'catchup': False,
    'tags': ['anomalies'],
    'max_active_runs': 1,
}

QUERY = '''
    WITH feed_metrics AS (
        SELECT
            toStartOfFifteenMinutes(time) AS start_of_15min
            , uniqExact(user_id) AS users_feed
            , countIf(action = 'view') AS views
            , countIf(action = 'like') AS likes
            , likes / views as ctr
        FROM 
            feed_actions
        WHERE 
            toDate(time) >= toDate(now()) - 59
            AND toHour(time) = toHour(now() - INTERVAL 15 MINUTE)
            AND toMinute(time) >= toMinute(toStartOfFifteenMinutes(now() - INTERVAL 15 MINUTE))
            AND toMinute(time) < toMinute(toStartOfFifteenMinutes(now() - INTERVAL 15 MINUTE)) + 15
        GROUP BY 
            start_of_15min
    )
    , messenger_metrics AS (
        SELECT
            toStartOfFifteenMinutes(time) AS start_of_15min
            , uniqExact(user_id) AS users_messenger
            , count() AS messages
        FROM 
            message_actions
        WHERE 
            toDate(time) >= toDate(now()) - 59
            AND toHour(time) = toHour(now() - INTERVAL 15 MINUTE)
            AND toMinute(time) >= toMinute(toStartOfFifteenMinutes(now() - INTERVAL 15 MINUTE))
            AND toMinute(time) < toMinute(toStartOfFifteenMinutes(now() - INTERVAL 15 MINUTE)) + 15
        GROUP BY 
            start_of_15min
    )
    SELECT
        if(f.start_of_15min != toDate(0), f.start_of_15min, m.start_of_15min) AS start_of_15min
        , f.users_feed
        , m.users_messenger
        , f.views
        , f.likes
        , f.likes / nullIf(f.views, 0) AS ctr
        , m.messages
    FROM 
        feed_metrics f
        FULL OUTER JOIN messenger_metrics m ON f.start_of_15min = m.start_of_15min
    ORDER BY 
        start_of_15min
'''

METRIC_CONFIG = {
    'users_feed': {
        'name': 'Feed Users',
        'threshold': 4.5,  
        'window_size': 30,
        'smooth_bounds': True,
        'smooth_window': 3,
        'format': '{:.0f}',  
    },
    'users_messenger': {
        'name': 'Messenger Users', 
        'threshold': 4.5,
        'window_size': 30,
        'smooth_bounds': True,
        'smooth_window': 3,
        'format': '{:.0f}',
    },
    'views': {
        'name': 'Post Views',
        'threshold': 4.0,
        'window_size': 30,
        'smooth_bounds': True, 
        'smooth_window': 3,
        'format': '{:.0f}',
    },
    'likes': {
        'name': 'Post Likes',
        'threshold': 4.0,
        'window_size': 30,
        'smooth_bounds': True,
        'smooth_window': 3,
        'format': '{:.0f}',
    },
    'ctr': {
        'name': 'CTR',
        'threshold': 4.0,  
        'window_size': 30,
        'smooth_bounds': True,
        'smooth_window': 3,
        'format': '{:.2%}', 
    },
    'messages': {
        'name': 'Messages Sent',
        'threshold': 4.0, 
        'window_size': 30,
        'smooth_bounds': True,
        'smooth_window': 3,
        'format': '{:.0f}',
    }
}

def handle_failure(context):
    """
    Callback function for processing unsuccessful tasks
    """
    exception = context.get('exception')
    task_instance = context['task_instance']

    logger.error(f"Task {task_instance.task_id} failed:")
    logger.error(f"Error: {exception}")
    logger.error(f"Execution date: {context['ds']}")
    logger.error(f"Attempt: {context['ti'].try_number}")

@dag(**dag_config)
def alert_system():
    """
    DAG runs every 15 minutes, extracts data from database, 
    checks for anomalies, and sends alerts to Telegram if any detected.
    """
    # ==========================================================================
    # Extract
    # ==========================================================================    
    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure
    )
    def extract() -> pd.DataFrame:
        """Extracts metrics data from database"""
        return (
            db.get_df(query=QUERY)
            .set_index('start_of_15min')
        )
            
    # ==========================================================================
    # Transform
    # ==========================================================================

    def make_calculate_bounds_task(metric: str):
        @task(
            task_id=f'calculate_{metric}_bounds',
            retries=3,
            retry_delay=timedelta(minutes=5),
            on_failure_callback=handle_failure
        )
        def calculate_single_bounds(df: pd.DataFrame) -> pd.DataFrame:
            """Calculate bounds for single metric"""
            logger.info(f"📊 Calculating bounds for {metric}")
            config = METRIC_CONFIG[metric]
            return calculate_anomaly_bounds_mad(
                df[metric],
                threshold=config['threshold'],
                window_size=config['window_size'],
                smooth_bounds=config['smooth_bounds'],
                smooth_window=config['smooth_window']
            )
        return calculate_single_bounds
    
    def make_metric_alert_task(metric: str):
        @task(
            task_id=f'create_{metric}_alert',
            retries=3,
            retry_delay=timedelta(minutes=5),
            on_failure_callback=handle_failure,
            multiple_outputs=True
        )
        def create_alert_content(df_bounds: pd.DataFrame) -> dict:    
            """Creates alert message and chart for a specific metric"""
            logger.info(f"📊 Creating alert content for {metric}")
            config = METRIC_CONFIG[metric] 
            value_format = config.get('format', '{:.0f}')
            last_timestamp = df_bounds.index[-1]
            last_row = df_bounds.iloc[-1]
            last_value = last_row['value']
            previous_value = df_bounds.iloc[-2]['value']
            last_lower_bound = last_row['lower_bound']
            last_upper_bound = last_row['upper_bound']
            metric_name = config['name']

            is_anomaly = not (last_lower_bound <= last_value <= last_upper_bound)
            
            if is_anomaly:
                logger.warning(f"🚨 Anomaly detected in {metric_name}")
                
                if last_value < last_lower_bound:
                    if last_lower_bound > 0:
                        deviation_from_bound = ((last_lower_bound - last_value) / last_lower_bound) * 100
                    else:   
                        deviation_from_bound = 100.0    
                    direction = "below"
                else:  
                    if last_upper_bound > 0:
                        deviation_from_bound = ((last_value - last_upper_bound) / last_upper_bound) * 100
                    else:
                        deviation_from_bound = 100.0                        
                    direction = "above" 
                
                if previous_value > 0:
                    daily_change = ((last_value - previous_value) / previous_value) * 100
                    deviation_from_yesterday = f"Change from yesterday: {daily_change:+.1f}%"    
                else:
                    deviation_from_yesterday = "Change from yesterday: N/A (no previous data)"
                time_slot = f"{last_timestamp.strftime('%H:%M')} - {(last_timestamp + pd.Timedelta(minutes=15)).strftime('%H:%M')}"                     
                # Create alert message
                msg = dedent(f"""
                    🚨 Anomaly Alert
                    
                    ⏰ Time slot: {time_slot}
                    📊 Metric: {metric_name}
                    🔢 Current value: {value_format.format(last_value)} 
                    ⚠️ Deviation: {deviation_from_bound:.1f}% {direction} range
                    📅 {deviation_from_yesterday}
                    ↔️ Expected range: {value_format.format(last_lower_bound)} - {value_format.format(last_upper_bound)}
                    📈 [Go To Dash](https://superset.lab.karpov.courses/superset/dashboard/7569/)
                    🕵️ @alert_jedis @avengers_analytics
                """)
                
                # Create chart
                fig = create_anomaly_chart(
                    bounds_df=df_bounds,
                    metric_name=metric_name
                )
                return {"message": msg, "chart": fig, "has_anomaly": True}
            
            logger.info(f"✅ No anomaly in {metric_name}")
            
            return {"message": None, "chart": None, "has_anomaly": False}
        return create_alert_content

    def make_send_message_task(metric: str):
        @task(
            task_id=f'send_{metric}_message',
            retries=3,
            retry_delay=timedelta(minutes=5),
            on_failure_callback=handle_failure
        )
        def send_alert_message(message: str) -> None:
            """Sends alert message for specific metric"""
            if message:
                logger.info(f"📨 Sending {metric} alert message...")
                if not bot.send_message(message=message):
                    logger.error(f"❌ Failed to send {metric} message")
                    raise Exception(f"Failed to send {metric} message")
                logger.info(f"✅ {metric} message sent successfully")
        return send_alert_message

    def make_send_chart_task(metric: str):
        @task(
            task_id=f'send_{metric}_chart',
            retries=3,
            retry_delay=timedelta(minutes=5),
            on_failure_callback=handle_failure
        )
        def send_alert_chart(figure: go.Figure) -> None:
            """Sends alert chart for specific metric"""
            if figure:
                logger.info(f"📊 Sending {metric} chart...")
                if not bot.send_chart(figure=figure):
                    logger.error(f"❌ Failed to send {metric} chart")
                    raise Exception(f"Failed to send {metric} chart")
                logger.info(f"✅ {metric} chart sent successfully")
        return send_alert_chart

    # ==========================================================================
    # WORKFLOW
    # ==========================================================================
    
    # Extract data
    df = extract()

    # Creating a chain of dependencies between graphs
    previous_chart_task = None

    # Create and send alerts for each metric
    for metric in METRIC_CONFIG.keys():
            
        # Create task instances for this metric
        bounds_task = make_calculate_bounds_task(metric)
        alert_task = make_metric_alert_task(metric)
        send_message_task = make_send_message_task(metric)
        send_chart_task = make_send_chart_task(metric)
        
        # Execute workflow for this metric
        df_bounds = bounds_task(df)
        alert_content = alert_task(df_bounds)
        send_message = send_message_task(alert_content['message'])
        send_chart = send_chart_task(alert_content['chart'])
        
        # Execution order
        send_message >> send_chart
        
        # Between metrics: graphs are sent sequentially
        if previous_chart_task:
            previous_chart_task >> send_chart
            
        previous_chart_task = send_chart

# Initialize DAG
alert_system = alert_system()