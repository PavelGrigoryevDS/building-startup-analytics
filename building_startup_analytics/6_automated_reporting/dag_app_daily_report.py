from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from telegram import InputFile
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from textwrap import dedent
from dotenv import load_dotenv
import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
from utils_for_dags import (
    ChConnector
    , TelegramBot
    , calc_wow
    , format_number
    , format_metrics_report_wow
    , prepare_comparison_df
    , create_comparison_dashboard
    , create_retention_dashboard
    , complete_missing_dates    
)
from app_daily_report_queries import (
    QUERY_DAU
    , QUERY_NEW_USERS
    , QUERY_ACTIVITY
    , QUERY_RETENTION
    , QUERY_ROLL_RETENTION_7D
    , QUERY_FEED_DETAILED
    , QUERY_MESSENGER_DETAILED
    , QUERY_USERS_DAILY_BY_SOURCE
)

db = ChConnector()
bot = TelegramBot()

default_args = {
    'owner': 'Pavel Grigoryev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 9, 25),
}

dag_config = {
    'default_args': default_args,
    'description': 'DAG for sending a daily report in Telegram',
    'schedule_interval': '0 11 * * *',
    'catchup': False,
    'tags': ['daily_report'],
    'max_active_runs': 1,
}

def handle_failure(context):
    """
    Callback function for processing unsuccessful tasks
    """
    exception = context.get('exception')
    task_instance = context['task_instance']

    print(f"Task {task_instance.task_id} It ended with an error:")
    print(f"Error: {exception}")
    print(f"Date of execution: {context['ds']}")
    print(f"Attempt №: {context['ti'].try_number}")

@dag(**dag_config)
def app_report():
    """
    DAG every day extracts data from the database, calculates metric, builds graphs, creates report and sends it to Telegram
    """
    # ==========================================================================
    # Extract
    # ==========================================================================
    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure
    )
    def extract_dau() -> pd.DataFrame:
        """
        Extracts data from the database
        """
        df = db.get_df(query=QUERY_DAU)
        return complete_missing_dates(df)

    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure
    )
    def extract_new_users() -> pd.DataFrame:
        """
        Extracts data from the database
        """
        df = db.get_df(query=QUERY_NEW_USERS)
        return complete_missing_dates(df)

    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure
    )
    def extract_activity() -> pd.DataFrame:
        """
        Extracts data from the database
        """
        df = db.get_df(query=QUERY_ACTIVITY)
        return complete_missing_dates(df)

    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure
    )
    def extract_retention() -> pd.DataFrame:
        """
        Extracts data from the database
        """
        return db.get_df(query=QUERY_RETENTION)

    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure
    )
    def extract_roll_retention_7d() -> pd.DataFrame:
        """
        Extracts data from the database
        """
        return db.get_df(query=QUERY_ROLL_RETENTION_7D)

    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure
    )
    def extract_feed_detailed() -> pd.DataFrame:
        """
        Extracts data from the database
        """
        df = db.get_df(query=QUERY_FEED_DETAILED)
        return complete_missing_dates(df)

    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure
    )
    def extract_messenger_detailed() -> pd.DataFrame:
        """
        Extracts data from the database
        """
        df = db.get_df(query=QUERY_MESSENGER_DETAILED)
        return complete_missing_dates(df)

    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure
    )
    def extract_users_daily_by_source() -> pd.DataFrame:
        """
        Extracts data from the database
        """
        return db.get_df(query=QUERY_USERS_DAILY_BY_SOURCE)

    # ==========================================================================
    # Transform
    # ==========================================================================

    # DAU
    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure,
        multiple_outputs=True
    )
    def transform_dau(df: pd.DataFrame) -> dict:
        """
        Calculates the growth of WOW
        """
        context = get_current_context()
        yesterday = context['execution_date']  # execution_date is period start for schedule_interval = '0 8 * * *'
        date_str = yesterday.strftime('%d %b %Y')          
        df_wow = calc_wow(df)
        df_comparison = prepare_comparison_df(
            df=df
            , date_col='date'
            , id_var='week_status'
            , value_in_id_var_for_text='current'
            , has_previous=True
        )
        metrics = {
            'total_dau': ['Total', '• 👥'],
            'feed_only_dau': ['Feed Only', '• 📰'],
            'messenger_only_dau': ['Messenger Only', '• 💬'],
            'both_services_dau': ['Both Services', '• 🔄']
        }
        msg = format_metrics_report_wow(df_wow, metrics, f'📊 App Report 📅 {date_str}\n\n👤 Daily Active Users')

        metrics = ['total_dau', 'feed_only_dau', 'messenger_only_dau', 'both_services_dau']
        metric_titles = ['Total DAU', 'Feed Only DAU', 'Messenger Only DAU', 'Both Services DAU']
        category_orders={'variable': metrics, 'week_status': ['current', 'previous']}

        fig = create_comparison_dashboard(
            df=df_comparison
            , date_col='date'
            , color='week_status'
            , text='text'
            , metrics=metrics
            , metric_titles=metric_titles
            , trace_name_for_text='current'
            , title='Daily Active Users'
            , labels={'date': 'Date'}
            , category_orders=category_orders
            , trace_names_map={'current': 'Current Week', 'previous': 'Previous Week'}
            , make_gray=True
        )
        return {"msg": msg, "fig": fig}

    # New Users
    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure,
        multiple_outputs=True
    )
    def transform_new_users(df: pd.DataFrame) -> dict:
        """
        Calculates the growth of WOW
        """       
        df_wow = calc_wow(df)
        df_comparison = prepare_comparison_df(
            df=df
            , date_col='date'
            , id_var='week_status'
            , value_in_id_var_for_text='current'
            , has_previous=True
        )
        metrics = {
            'all_new_users': ['All New Users', '• 👥'],
            'feed_new_users': ['Feed New Users', '• 📰'],
            'messenger_new_users': ['Msg New Users', '• 💬'],
            'both_first_users': ['Used Both First Time', '• 🔄']
        }
        msg = format_metrics_report_wow(df_wow, metrics, '🆕 New Users & First Usage')

        metrics = ['all_new_users', 'feed_new_users', 'messenger_new_users', 'both_first_users']
        metric_titles = ['All New Users', 'Feed New Users', 'Msg New Users', 'Used Both First Time']
        category_orders={'variable': metrics, 'week_status': ['current', 'previous']}

        fig = create_comparison_dashboard(
            df=df_comparison
            , date_col='date'
            , color='week_status'
            , text='text'
            , metrics=metrics
            , metric_titles=metric_titles
            , trace_name_for_text='current'
            , title='New Users Activity'
            , labels={'date': 'Date'}
            , category_orders=category_orders
            , trace_names_map={'current': 'Current Week', 'previous': 'Previous Week'}
            , make_gray=True
        )
        return {"msg": msg, "fig": fig}

    # Activity
    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure,
        multiple_outputs=True
    )
    def transform_activity(df: pd.DataFrame) -> dict:
        """
        Calculates the growth of WOW
        """
        df_wow = calc_wow(df)
        df_comparison = prepare_comparison_df(
            df=df
            , date_col='date'
            , id_var='week_status'
            , value_in_id_var_for_text='current'
            , has_previous=True
        )

        metrics = {
            'views': ['Views', '• 👀'],
            'likes': ['Likes', '• ❤️'],
            'ctr': ['CTR', '• 🎯'],
            'messages': ['Messages', '• ✉️']
        }
        msg = format_metrics_report_wow(df_wow, metrics, '🔥 Activity Metrics')

        metrics = ['views', 'likes', 'ctr', 'messages']
        metric_titles = ['Views', 'Likes', 'CTR', 'Messages']
        category_orders={'variable': metrics, 'week_status': ['current', 'previous']}

        fig = create_comparison_dashboard(
            df=df_comparison
            , date_col='date'
            , color='week_status'
            , text='text'
            , metrics=metrics
            , metric_titles=metric_titles
            , trace_name_for_text='current'
            , title='Activity Metrics'
            , labels={'date': 'Date'}
            , category_orders=category_orders
            , trace_names_map={'current': 'Current Week', 'previous': 'Previous Week'}
            , make_gray=True
        )
        return {"msg": msg, "fig": fig}

    # Retention
    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure,
        multiple_outputs=True
    )
    def transform_retention(df: pd.DataFrame, df_roll: pd.DataFrame) -> dict:
        """
        Creates a DataFrame for comparison
        """
        df_cohort = df.pivot_table(index='cohort', columns='lifetime', values='users', fill_value=0)
        df_retention = (
            df_cohort.div(df_cohort[0], axis=0)
            .drop(0, axis=1)
        )

        df_retention_7d = df_retention[7]
        df_retention_7d.index = df_retention_7d.index.strftime('%b %d')
        mean_retention = df_retention_7d.mean()
        best_cohort = df_retention_7d.agg(['idxmax', 'max'])
        worst_cohort = df_retention_7d.agg(['idxmin', 'min'])
        spread = abs(best_cohort['max'] - worst_cohort['min']) * 100
        yesterday_dau = format_number(int(df_roll['yesterday_users'].iloc[0]))
        all_week_users = format_number(int(df_roll['all_week_users'].iloc[0]))
        msg = dedent(f"""
        🎯 Retention Analysis

        7-Day Cohort Retention
        (Last 7 completed cohorts)
        • ⚖️ Mean: {mean_retention:.1%}
        • 🏆 Best Cohort: {best_cohort['max']:.1%} ({best_cohort['idxmax']})
        • ⚠️ Worst Cohort: {worst_cohort['min']:.1%} ({worst_cohort['idxmin']})
        • ↔️ Spread: {spread:.1f} pp

        Rolling Retention 7D (Current Audience)
        • 👥 Yesterday's DAU: {yesterday_dau}
        • 📅 Active in last 7 days: {all_week_users}
        • 🎯 Rolling Retention: {df_roll['retention_7d'].iloc[0]:.1%}
        """)

        fig = create_retention_dashboard(df_retention=df_retention)
        return {"msg": msg, "fig": fig}

    # Feed Detailed
    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure,
        multiple_outputs=True
    )
    def transform_feed(df: pd.DataFrame) -> dict:
        """
        Calculates the growth of WOW
        """
        df_wow = calc_wow(df)
        df_comparison = prepare_comparison_df(
            df=df
            , date_col='date'
            , id_var='week_status'
            , value_in_id_var_for_text='current'
            , has_previous=True
        )

        metrics = {
            'posts_per_user': ['Posts per User', '• 📝'],
            'views_per_user': ['Views per User', '• 👀'],
            'likes_per_user': ['Likes per User', '• ❤️'],
            'ctr_per_user': ['CTR per User', '• 🎯']
        }
        msg = format_metrics_report_wow(df_wow, metrics, '📰 Feed Detailed')

        metrics = ['posts_per_user', 'views_per_user', 'likes_per_user', 'ctr_per_user']
        metric_titles = ['Posts per User', 'Views per User', 'Likes per User', 'CTR per User']
        category_orders={'variable': metrics, 'week_status': ['current', 'previous']}

        fig = create_comparison_dashboard(
            df=df_comparison
            , date_col='date'
            , color='week_status'
            , text='text'
            , metrics=metrics
            , metric_titles=metric_titles
            , trace_name_for_text='current'
            , title = 'Feed Detailed Metrics'
            , labels={'date': 'Date'}
            , category_orders=category_orders
            , trace_names_map={'current': 'Current Week', 'previous': 'Previous Week'}
            , tickformats=[['.1%', 1, 2]]
            , make_gray=True
        )
        return {"msg": msg, "fig": fig}

    # Messenger Detailed
    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure,
        multiple_outputs=True
    )
    def transform_messenger(df: pd.DataFrame) -> dict:
        """
        Calculates the growth of WOW
        """
        df_wow = calc_wow(df)
        df_comparison = prepare_comparison_df(
            df=df
            , date_col='date'
            , id_var='week_status'
            , value_in_id_var_for_text='current'
            , has_previous=True
        )

        metrics = {
            'sender_dau': ['Sender DAU', '• 📢'],
            'receiver_dau': ['Receiver DAU', '• 📭'],
            'sender_to_receiver_ratio': ['Sender DAU / Receiver DAU', '• ⚖️'],
            'messages_per_sender': ['Messages per Sender', '• ✉️']
        }
        msg = format_metrics_report_wow(df_wow, metrics, '💬 Messenger Detailed')

        metrics = ['sender_dau', 'receiver_dau', 'sender_to_receiver_ratio', 'messages_per_sender']
        metric_titles = ['Sender DAU', 'Receiver DAU', 'Sender DAU / Receiver DAU', 'Messages per Sender']
        category_orders={'variable': metrics, 'week_status': ['current', 'previous']}

        fig = create_comparison_dashboard(
            df=df_comparison
            , date_col='date'
            , color='week_status'
            , text='text'
            , metrics=metrics
            , metric_titles=metric_titles
            , trace_name_for_text='current'
            , title = 'Messenger Detailed Metrics'
            , labels={'date': 'Date'}
            , category_orders=category_orders
            , trace_names_map={'current': 'Current Week', 'previous': 'Previous Week'}
            , make_gray=True
        )
        return {"msg": msg, "fig": fig}

    # DAU by Source
    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure,
        multiple_outputs=True
    )
    def transform_by_source(df: pd.DataFrame) -> dict:
        """
        Creates a DataFrame for comparison
        """
        df_comparison = prepare_comparison_df(
            df=df
            , date_col='date'
            , id_var='source'
            , has_previous=False
        )

        mask = df['date'] == df['date'].max()
        latest_data = df[mask]

        total_dau = latest_data['total_dau'].sum()
        ads_dau = latest_data[latest_data['source'] == 'ads']['total_dau'].iloc[0]
        organic_dau = latest_data[latest_data['source'] == 'organic']['total_dau'].iloc[0]

        ads_share = ads_dau / total_dau
        organic_share = organic_dau / total_dau

        ads_feed_share = latest_data[latest_data['source'] == 'ads']['feed_only_dau'].iloc[0] / ads_dau
        ads_messenger_share = latest_data[latest_data['source'] == 'ads']['messenger_only_dau'].iloc[0] / ads_dau
        ads_both_share = latest_data[latest_data['source'] == 'ads']['both_services_dau'].iloc[0] / ads_dau

        organic_feed_share = latest_data[latest_data['source'] == 'organic']['feed_only_dau'].iloc[0] / organic_dau
        organic_messenger_share = latest_data[latest_data['source'] == 'organic']['messenger_only_dau'].iloc[0] / organic_dau
        organic_both_share = latest_data[latest_data['source'] == 'organic']['both_services_dau'].iloc[0] / organic_dau

        msg =  dedent(f"""
            🌐 Active Users by Source 

            👥 Total DAU: {total_dau:,.0f}
            • Ads: {ads_share:.1%} ({ads_dau:,.0f})
            • Organic: {organic_share:.1%} ({organic_dau:,.0f})
            📰 Feed Only:
            • Ads: {ads_feed_share:.1%} • Organic: {organic_feed_share:.1%}
            💬 Messenger Only:
            • Ads: {ads_messenger_share:.1%} • Organic: {organic_messenger_share:.1%}
            🔄 Both Services:
            • Ads: {ads_both_share:.1%} • Organic: {organic_both_share:.1%}
        """)

        metrics = ['total_dau', 'feed_only_dau', 'messenger_only_dau', 'both_services_dau']
        metric_titles = ['Total DAU', 'Feed Only DAU', 'Messenger Only DAU', 'Both Services DAU']
        category_orders={'variable': metrics}
        trace_names_map={'ads': 'Ads', 'organic': 'Organic'} 

        fig = create_comparison_dashboard(
            df=df_comparison
            , date_col='date'
            , color='source'
            , metrics=metrics
            , metric_titles=metric_titles
            , title = 'Daily Active Users by Source'
            , labels={'date': 'Date'}
            , category_orders=category_orders
            , make_gray=False
            , trace_names_map=trace_names_map
        )
        return {"msg": msg, "fig": fig}

    # ==========================================================================
    # Load
    # ==========================================================================
    # DAU
    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure
    )
    def load_dau_message(message: str) -> None:
        """Task for sending DAU message"""
        print("📨 Sending DAU message...")
        if not bot.send_message(message=message):
            raise Exception("Failed to send DAU message")
        print("✅ DAU message sent successfully")

    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure
    )
    def load_dau_chart(figure: go.Figure) -> None:
        """Task for sending DAU chart"""
        print("📊 Sending DAU chart...")
        if not bot.send_chart(figure=figure):
            raise Exception("Failed to send DAU chart")
        print("✅ DAU chart sent successfully")

    # New users
    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure
    )
    def load_new_users_message(message: str) -> None:
        """Task for sending new users message"""
        print("📨 Sending new users message...")
        if not bot.send_message(message=message):
            raise Exception("Failed to send new users message")
        print("✅ new users message sent successfully")

    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure
    )
    def load_new_users_chart(figure: go.Figure) -> None:
        """Task for sending new users chart"""
        print("📊 Sending new users chart...")
        if not bot.send_chart(figure=figure):
            raise Exception("Failed to send new users chart")
        print("✅ new users chart sent successfully")

    # Activity
    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure
    )
    def load_activity_message(message: str) -> None:
        """Task for sending Activity message"""
        print("📨 Sending Activity message...")
        if not bot.send_message(message=message):
            raise Exception("Failed to send Activity message")
        print("✅ Activity message sent successfully")

    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure
    )
    def load_activity_chart(figure: go.Figure) -> None:
        """Task for sending Activity chart"""
        print("📊 Sending Activity chart...")
        if not bot.send_chart(figure=figure):
            raise Exception("Failed to send Activity chart")
        print("✅ Activity chart sent successfully")

    # Retention
    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure
    )
    def load_retention_message(message: str) -> None:
        """Task for sending Retention message"""
        print("📨 Sending Retention message...")
        if not bot.send_message(message=message):
            raise Exception("Failed to send Retention message")
        print("✅ Retention message sent successfully")

    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure
    )
    def load_retention_chart(figure: go.Figure) -> None:
        """Task for sending Retention chart"""
        print("📊 Sending Retention chart...")
        if not bot.send_chart(figure=figure):
            raise Exception("Failed to send Retention chart")
        print("✅ Retention chart sent successfully")

    # Feed Detailed
    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure
    )
    def load_feed_detailed_message(message: str) -> None:
        """Task for sending Feed Detailed message"""
        print("📨 Sending Feed Detailed message...")
        if not bot.send_message(message=message):
            raise Exception("Failed to send Feed Detailed message")
        print("✅ Feed Detailed message sent successfully")

    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure
    )
    def load_feed_detailed_chart(figure: go.Figure) -> None:
        """Task for sending Feed Detailed chart"""
        print("📊 Sending Feed Detailed chart...")
        if not bot.send_chart(figure=figure):
            raise Exception("Failed to send Feed Detailed chart")
        print("✅ Feed Detailed chart sent successfully")

    # Messenger Detailed
    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure
    )
    def load_messenger_detailed_message(message: str) -> None:
        """Task for sending Messenger Detailed message"""
        print("📨 Sending Messenger Detailed message...")
        if not bot.send_message(message=message):
            raise Exception("Failed to send Messenger Detailed message")
        print("✅ Messenger Detailed message sent successfully")

    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure
    )
    def load_messenger_detailed_chart(figure: go.Figure) -> None:
        """Task for sending Messenger Detailed chart"""
        print("📊 Sending Messenger Detailed chart...")
        if not bot.send_chart(figure=figure):
            raise Exception("Failed to send Messenger Detailed chart")
        print("✅ Messenger Detailed chart sent successfully")

    # Users by Source
    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure
    )
    def load_by_source_message(message: str) -> None:
        """Task for sending Users by Source message"""
        print("📨 Sending Users by Source message...")
        if not bot.send_message(message=message):
            raise Exception("Failed to send Users by Source message")
        print("✅ Users by Source message sent successfully")

    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=handle_failure
    )
    def load_by_source_chart(figure: go.Figure) -> None:
        """Task for sending Users by Source chart"""
        print("📊 Sending Users by Source chart...")
        if not bot.send_chart(figure=figure):
            raise Exception("Failed to send Users by Source chart")
        print("✅ Users by Source chart sent successfully")

    # ==========================================================================
    # WORKFLOW
    # ==========================================================================

    # extract
    df_dau = extract_dau()
    df_new_users = extract_new_users()
    df_activity = extract_activity()
    df_retention = extract_retention()
    df_roll_retention_7d = extract_roll_retention_7d()
    df_feed_detailed = extract_feed_detailed()
    df_messenger_detailed = extract_messenger_detailed()
    df_users_daily_by_source = extract_users_daily_by_source()

    # transform
    transform_dau_result = transform_dau(df_dau)
    msg_dau = transform_dau_result["msg"]
    fig_dau = transform_dau_result["fig"]

    transform_new_users_result = transform_new_users(df_new_users)
    msg_new_users = transform_new_users_result["msg"]
    fig_new_users = transform_new_users_result["fig"]    
    
    transform_activity_result = transform_activity(df_activity)
    msg_activity = transform_activity_result["msg"]
    fig_activity = transform_activity_result["fig"]   
     
    transform_retention_result = transform_retention(df_retention, df_roll_retention_7d)
    msg_retention = transform_retention_result["msg"]
    fig_retention = transform_retention_result["fig"]    
        
    transform_feed_detailed_result = transform_feed(df_feed_detailed)
    msg_feed_detailed = transform_feed_detailed_result["msg"]
    fig_feed_detailed = transform_feed_detailed_result["fig"]      
    
    transform_messenger_detailed_result = transform_messenger(df_messenger_detailed)
    msg_messenger_detailed = transform_messenger_detailed_result["msg"]
    fig_messenger_detailed = transform_messenger_detailed_result["fig"]       
    
    transform_by_source_result = transform_by_source(df_users_daily_by_source)
    msg_by_source = transform_by_source_result["msg"]
    fig_by_source = transform_by_source_result["fig"]      


    # load
    dau_msg_task = load_dau_message(msg_dau)
    dau_chart_task = load_dau_chart(fig_dau)

    new_users_msg_task = load_new_users_message(msg_new_users)  
    new_users_chart_task = load_new_users_chart(fig_new_users)
    
    activity_msg_task = load_activity_message(msg_activity)  
    activity_chart_task = load_activity_chart(fig_activity)    

    retention_msg_task = load_retention_message(msg_retention)
    retention_chart_task = load_retention_chart(fig_retention)

    feed_detailed_msg_task = load_feed_detailed_message(msg_feed_detailed)
    feed_detailed_chart_task = load_feed_detailed_chart(fig_feed_detailed)   
    
    messenger_detailed_msg_task = load_messenger_detailed_message(msg_messenger_detailed)
    messenger_detailed_chart_task = load_messenger_detailed_chart(fig_messenger_detailed)    
            
    by_source_msg_task = load_by_source_message(msg_by_source)
    by_source_chart_task = load_by_source_chart(fig_by_source)
    
    # task dependencies
    (
        dau_msg_task >> dau_chart_task >>
        new_users_msg_task >> new_users_chart_task >>
        activity_msg_task >> activity_chart_task >>
        retention_msg_task >> retention_chart_task >>
        feed_detailed_msg_task >> feed_detailed_chart_task >>
        messenger_detailed_msg_task >> messenger_detailed_chart_task >>
        by_source_msg_task >> by_source_chart_task
    )
      
# ==========================================================================
# DAG execution
# ==========================================================================

app_report = app_report()