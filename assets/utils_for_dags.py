"""
Utilities for Airflow DAGs
"""

from typing import Union
import pandas as pd
import numpy as np
import pandahouse as ph
from scipy import stats
import requests
from dotenv import load_dotenv
import io
import os
import sys
from telegram import Bot, InputFile
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
import plotly_config
env_path = os.path.join(current_dir, '.env')

load_dotenv(env_path)

# =============================================================================
# CONFIGURATION
# =============================================================================

class Config:
    """Loads credentials from .env file"""
    
    # Telegram Bot
    BOT_TOKEN = os.getenv('BOT_TOKEN')
    CHAT_ID = os.getenv('CHAT_ID')
    
    # Database
    DB_CONFIG = {
        'host': os.getenv('DB_HOST'),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'), 
        'password': os.getenv('DB_PASSWORD')
    }
    
    # Plotly API
    PLOTLY_USERNAME = os.getenv('PLOTLY_USERNAME')
    PLOTLY_API_KEY = os.getenv('PLOTLY_API_KEY')
    
    @classmethod
    def validate_config(cls):
        """Validate that all required environment variables are set"""
        required_vars = ['BOT_TOKEN', 'CHAT_ID', 'DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            raise ValueError(f"Missing required environment variables: {missing}")
        print("✅ All environment variables loaded successfully")

# Validate on import
Config.validate_config()

# =============================================================================
# DATABASE CONNECTOR
# =============================================================================

class ChConnector:
    """
    Simple database connector for ClickHouse queries
    Returns pandas DataFrame with query results
    """
    
    def __init__(self, db_config: dict = None):
        """
        Initialize with database configuration
        
        Args:
            db_config: Dictionary with host, database, user, password
        """
        self.db_config = db_config or Config.DB_CONFIG
    
    def get_df(self, query: str, database: str=None) -> pd.DataFrame:
        """
        Execute SQL query and return DataFrame
        
        Args:
            query: SQL query string
            
        Returns:
            pandas.DataFrame with query results
        """
        connection = self.db_config.copy()
        if database:
            connection['database'] = database
        try:
            df = ph.read_clickhouse(query, connection=connection)
            print(f"✅ Query executed successfully. Returned {len(df)} rows")
            return df
        except Exception as e:
            print(f"❌ Database query error: {e}")
            raise
    
    def test_connection(self, database: str=None) -> bool:
        """Test database connection with simple query"""
        try:
            result = self.get_df(query="SELECT 1 as connection_test", database=database)
            return True
        except:
            return False
        
# =============================================================================
# TELEGRAM BOT
# =============================================================================

class TelegramBot:
    """
    Telegram bot for sending messages and charts
    """
    
    def __init__(self, token: str = None, chat_id: str = None):
        """
        Initialize Telegram bot
        
        Args:
            token: Bot token from BotFather
            chat_id: Target chat/group ID
        """
        self.token = token or Config.BOT_TOKEN
        self.chat_id = chat_id or Config.CHAT_ID
        self.bot = Bot(token=self.token)
    
    def send_message(self, message: str, chat_id: str=None, parse_mode: str='Markdown') -> bool:
        """
        Send text message to Telegram
        
        Args:
            message: Text message to send
            chat_id: Target chat ID (uses default if not provided)
            
        Returns:
            Success status
        """
        try:
            target_chat = chat_id or self.chat_id
            self.bot.send_message(
                chat_id=target_chat,
                text=message,
                parse_mode=parse_mode
            )
            print(f"✅ Message sent to chat {target_chat}")
            return True
        except Exception as e:
            print(f"❌ Error sending message: {e}")
            return False
    
    def send_chart(self, figure: go.Figure, caption: str = "", 
                         chat_id: str = None, width: int = None, height: int = None) -> bool:
        """
        Send Plotly chart as image to Telegram
        
        Args:
            figure: Plotly Figure object
            caption: Image caption
            chat_id: Target chat ID
            width: Chart width in pixels
            height: Chart height in pixels
            
        Returns:
            Success status
        """
        try:
            # Convert Plotly figure to JSON
            plot_json = figure.to_json()
            
            # Send to Plotly API for PNG conversion
            payload = {
                'figure': plot_json,
                'format': 'png',
                'width': width,
                'height': height
            }
            
            response = requests.post(
                'https://api.plot.ly/v2/images',
                json=payload,
                headers={'Plotly-Client-Platform': 'python'},
                auth=(Config.PLOTLY_USERNAME, Config.PLOTLY_API_KEY),
                timeout=30
            )
            if response.status_code == 200:
                # Create in-memory image and send to Telegram
                buffer = io.BytesIO(response.content)
                buffer.seek(0)
                
                target_chat = chat_id or self.chat_id
                self.bot.send_photo(
                    chat_id=target_chat,
                    photo=InputFile(buffer, filename='chart.png'),
                    caption=caption,
                    parse_mode='HTML'
                )
                print(f"✅ Chart sent to chat {target_chat}")
                return True
            else:
                print(f"❌ Plotly API error: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Error sending chart: {e}")
            return False
    
    def send_csv(self, df: pd.DataFrame, filename: str = "data.csv", 
                caption: str = "", chat_id: str = None) -> bool:
        """
        Send DataFrame as CSV file to Telegram
        
        Args:
            df: DataFrame to send as CSV
            filename: Name for the CSV file
            caption: File caption
            chat_id: Target chat ID
            
        Returns:
            Success status
        """
        try:
            # Create CSV in memory
            buffer = io.BytesIO()
            df.to_csv(buffer, index=False, encoding='utf-8')
            buffer.seek(0)
            
            target_chat = chat_id or self.chat_id
            self.bot.send_document(
                chat_id=target_chat,
                document=InputFile(buffer, filename=filename),
                caption=caption,
                parse_mode='HTML'
            )
            print(f"✅ CSV file '{filename}' sent to chat {target_chat}")
            return True
        except Exception as e:
            print(f"❌ Error sending CSV file: {e}")
            return False
        
    def get_chat_info(self, chat_id: str=None, return_result: bool=False) -> Union[dict, None]:
        """
        Get detailed information about chat
        
        Args:
            chat_id: Target chat ID (uses default if not provided)
            
        Returns:
            Dictionary with chat information
        """
        try:
            target_chat = chat_id or self.chat_id
            chat = self.bot.get_chat(chat_id=target_chat)
            
            chat_info = {
                'id': chat.id,
                'type': chat.type,
                'title': getattr(chat, 'title', 'N/A'),
                'username': getattr(chat, 'username', 'N/A'),
                'first_name': getattr(chat, 'first_name', 'N/A'),
                'last_name': getattr(chat, 'last_name', 'N/A'),
                'description': getattr(chat, 'description', 'N/A'),
                'members_count': getattr(chat, 'members_count', 'N/A'),
                'invite_link': getattr(chat, 'invite_link', 'N/A'),
                'pinned_message': getattr(chat, 'pinned_message', 'N/A'),
                'permissions': getattr(chat, 'permissions', 'N/A'),
                'bio': getattr(chat, 'bio', 'N/A'),
            }
            
            print(f"📊 Chat info for {target_chat}:")
            for key, value in chat_info.items():
                print(f"  {key}: {value}")
            if return_result:    
                return chat_info
            
        except Exception as e:
            print(f"❌ Error getting chat info: {e}")
            return {}        

# =============================================================================
# Additional functions
# =============================================================================        
        
def format_number(value, variable_type=None):
    """Formats numbers for beautiful display"""
    if pd.isna(value):
        return ""
    
    if variable_type and 'ctr' in variable_type or variable_type == 'ratio':
        return f"{value:.1%}"
    elif variable_type == 'rate':
        return f"{value:.2f}"
    elif isinstance(value, (int, float)):
        if abs(value) >= 1e6:
            return f"{value/1e6:.1f}M"
        elif abs(value) >= 1e3:
            return f"{value/1e3:.0f}K"
        else:
            return f"{value:.0f}"
    else:
        return str(value)        

def complete_missing_dates(df, date_column='date', days_back=13):
    """
    Complete missing dates in dataframe with zeros and current week status.
    """
    # Create full date range
    end_date = datetime.now().date() - pd.Timedelta(days=1)
    start_date = end_date - pd.Timedelta(days=days_back)
    full_range = pd.date_range(start=start_date, end=end_date, name=date_column)
    
    # Reindex to complete missing dates
    df_complete = (
        df
        .sort_values(date_column)
        .set_index(date_column)
        .reindex(full_range)
        .reset_index()
    )
    
    # Fill missing values
    df_complete['week_status'] = df_complete['week_status'].fillna('current')
    df_complete = df_complete.fillna(0)
    
    return df_complete
    
def calc_wow(df: pd.DataFrame, date_col: str='date') -> pd.DataFrame:
    """Add wow to DataFrame"""
    df = df.sort_values(date_col)
    df_wow = (
        df.iloc[[6, 13]]
        .drop(date_col, axis=1)
        .set_index('week_status')
    )
    df_wow.loc['wow'] = (df_wow.loc['current'].astype('float') - df_wow.loc['previous'].astype('float')) / df_wow.loc['previous'].astype('float')
    return df_wow    
    
def format_metrics_report_wow(df: pd.DataFrame, metrics: dict, header: str=None):
    """
    Uses symbols for alignment in Telegram
    """
    report_lines = []
    if header:
        report_lines.append(header + '\n')
    for metric in metrics.keys():
        current_value = df.loc['current', metric]
        current_value = format_number(value=current_value, variable_type=metric)
        wow_change = df.loc['wow', metric]
        circle = "🟢" if wow_change > 0 else "🔴" if wow_change < 0 else "⚪"
        line = f"{metrics[metric][1]} {metrics[metric][0]}: {current_value} {circle} {wow_change:+.1%} WoW"
        report_lines.append(line)
    
    return "\n".join(report_lines)       
    
def prepare_comparison_df(
    df: pd.DataFrame
    , date_col: str
    , id_var: str
    , has_previous: bool
    , value_in_id_var_for_text: str=None
    , text_format: str=None
    ) -> pd.DataFrame:
    """Prepares data for comparing the current and previous week"""
    if id_var not in df.columns:
        raise ValueError(f"DataFrame must contain {id_var} column")    
    if has_previous:
        df.loc[df[id_var] == 'previous', date_col] += pd.Timedelta(days=7)
    df = (
        df.melt(
            id_vars=[date_col, id_var]
        ) 
    )
    if value_in_id_var_for_text:
        mask = df[id_var]==value_in_id_var_for_text
        mask &= df.groupby('variable')[date_col].transform(
            lambda x: x.isin([x.min(), x.max()])
        )
        df['text'] = df['value'].where(mask)
        if text_format:
            df['text'] = df['text'].apply(lambda x: f"{x:{text_format}}")
        else:
            df['text'] = [format_number(v, var) for v, var in zip(df['text'], df['variable'])]
    return df    

def create_comparison_dashboard(
    df: pd.DataFrame
    , date_col: str
    , color: str
    , metrics: list
    , metric_titles: list
    , title: str
    , make_gray: bool
    , text: str=None
    , category_orders: dict=None
    , tickformats: list=None
    , trace_name_for_text: str=None
    , labels: dict=None
    , trace_names_map: dict=None
    ) -> go.Figure:
    """Creates a 2x2 dashboard for comparing metrics"""
    fig = px.line(
        df
        , x=date_col
        , y='value'
        , color=color
        , text=text
        , facet_col='variable'
        , facet_col_wrap=2
        , facet_col_spacing=0.08
        , facet_row_spacing=0.15
        , category_orders=category_orders
        , labels=labels
        , line_shape='spline'
        , title=title
        , width=1000
        , height=600
    )
    # Set up lines
    added_legends = []
    for trace in fig.data:
        # Update trace
        trace.showlegend = False
        trace.mode = 'lines+markers'
        if make_gray:
            if trace.name == trace_name_for_text:
                trace.update(
                    mode='lines+markers+text',
                    textposition='top center',
                    line=dict(color='#777777'),
                    marker=dict(color='#777777'),
                )
            else:  
                trace.update(
                    line=dict(color='#C1C1C1', dash='dash'), 
                    marker=dict(color='#C1C1C1'),       
                )
        if trace.name not in added_legends:
            added_legends.append(trace.name)
            legend_name = trace_names_map[trace.name] if trace_names_map else trace.name
            fig.add_trace(go.Scatter(
                x=[None], y=[None],
                mode=trace.mode.replace('+text', ''),
                name=legend_name,  
                legendgroup=trace.legendgroup,
                showlegend=True,
                line=dict(color=trace.line.color)
            ))                
    fig.update_layout(legend_title=None, legend_itemsizing='constant')
    fig.update_xaxes(matches=None, tickformat='%b %d', dtick='1D', showticklabels=True)
    fig.update_yaxes(matches=None, showticklabels=True, title=None)
    # Use formats of values ​​if indicated
    if tickformats:
        for tickformat, row, col in tickformats:
            fig.update_yaxes(tickformat=tickformat, row=row, col=col)
    # Update the headlines of the metrics
    # Correct order for 2x2 nets
    metric_titles = [metric_titles[2], metric_titles[3], metric_titles[0], metric_titles[1]]
    for i, annotation in enumerate(fig.layout.annotations):
        annotation.text = metric_titles[i]
    # Adjust Y-axis ranges to accommodate text labels
    # Add 10% padding based on data range for each subplot
    if trace_name_for_text:
        yaxis_map = {metrics[0]: 'yaxis3', metrics[1]: 'yaxis4', metrics[2]: 'yaxis', metrics[3]: 'yaxis2'}
        for variable in df['variable'].unique():
            subset = df[df['variable'] == variable]
            min_val = subset['value'].min()
            max_val = subset['value'].max()
            delta = (max_val - min_val) * 0.1  
            yaxis_name = yaxis_map[variable]
            
            if yaxis_name in fig.layout:
                fig.layout[yaxis_name]['range'] = (min_val - delta, max_val + delta)
    return fig

def create_retention_dashboard(df_retention):
    """Create Dashboard with Retention Heatmap and Line Chart"""
    
    # Data preparation for Line Chart
    df_retention_7d = (
        df_retention
        .reset_index()
        .rename_axis(columns=None)    
        [['cohort', 7]]
        .copy()
        .rename(columns={7: 'retention_7_day'})
    )
    df_retention_7d['text'] = np.nan
    df_retention_7d.loc[[0, df_retention_7d.index[-1]], 'text'] = (
        df_retention_7d['retention_7_day']
        .apply(lambda x: f"{x:.1%}")
    )
    
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=('Retention First Week Performance', '7-Day Retention by Cohort'),
        horizontal_spacing=0.15
    )
    
    heatmap_fig = go.Heatmap(
        z=df_retention.values,
        x=df_retention.columns,
        y=df_retention.index.strftime('%b %d'),
        colorscale='Greens',
        colorbar=dict(
            tickformat='.0%',
            x=0.43,  
            y=0.5,  
            xanchor='left',  
            yanchor='middle', 
            thickness=15,  
        ),
    )
    fig.add_trace(heatmap_fig, row=1, col=1)
    
    # Right figure - Line Chart
    line_fig = go.Scatter(
        x=df_retention_7d['cohort'],
        y=df_retention_7d['retention_7_day'],
        mode='lines+markers+text',
        text=df_retention_7d['text'],
        textposition='top center',
        line=dict(color='#777777', shape='spline'),
        marker=dict(color='#777777')
    )
    fig.add_trace(line_fig, row=1, col=2)
    
    # Update Layout
    fig.update_layout(
        height=400,
        width=1100,
        showlegend=False,
        title_text="Retention Analysis Dashboard",
        margin=dict(l=90, r=20)
    )
    
    # Setting axes for Heatmap (left figure)
    fig.update_xaxes(
        title_text='Lifetime',
        title_standoff=7,
        type='category', 
        showgrid=False,
        row=1, col=1
    )
    fig.update_yaxes(
        title_text='Cohort',
        title_standoff=10,
        type='category',  
        showgrid=False,
        autorange='reversed',
        row=1, col=1
    )
    
    # Setting axes for Line Chart (right figure)
    fig.update_xaxes(
        title_text='Cohort',
        tickformat='%b %d',
        title_standoff=7,
        row=1, col=2
    )
    fig.update_yaxes(
        title_text='7-Day Retention',
        title_standoff=10,
        tickformat='.0%',
        row=1, col=2
    )
    vmax = df_retention.max().max()
    vmin = df_retention.min().min()
    center_color_bar = vmin + (vmax - vmin) * 0.7 if (vmax - vmin) > 0 else vmin
    
    for row in range(len(df_retention)):
        for col in range(len(df_retention.columns)):
            fig.add_annotation(
                text=f"{df_retention.values[row, col]:.0%}",
                x=col,
                y=row,
                xref="x",
                yref="y",
                showarrow=False,
                font=dict(
                    color="white" if df_retention.values[row, col] >= center_color_bar else "rgba(0, 0, 0, 0.7)",
                    size=10
                ),
                xanchor='center',
                yanchor='middle',
                row=1,  
                col=1  
            )
    return fig

def calculate_anomaly_bounds_mad(
    column: pd.Series, 
    window_size: int = 30,
    threshold: float = 3.0,
    smooth_bounds: bool = True,
    smooth_window: int = 3 
) -> pd.DataFrame:
    """
    Calculate anomaly bounds using Median Absolute Deviation (MAD) via rolling.
    
    Args:
        column: Time series data with datetime index
        window_size: Number of previous points for bounds calculation  
        threshold: MAD multiplier (default: 3.0 ~ 99.7% for normal distribution)
        smooth_bounds: Whether to smooth bounds (default: True)  
        smooth_window: Window size for smoothing (default: 3)
    """
    shifted = column.shift(1)
    rolling_median = shifted.rolling(window_size).median()
    rolling_mad = (
        shifted.rolling(window_size)
        .apply(lambda x: stats.median_abs_deviation(x, nan_policy="omit"))
    )
    # Converting MAD to "standard deviation"
    mad_std = rolling_mad * 1.4826
    
    lower_bounds = rolling_median - threshold * mad_std
    upper_bounds = rolling_median + threshold * mad_std
    if smooth_bounds:
        lower_bounds = lower_bounds.rolling(window=smooth_window, min_periods=1).mean()
        upper_bounds = upper_bounds.rolling(window=smooth_window, min_periods=1).mean()
    
    df_bounds = pd.DataFrame({
        'value': column,
        'lower_bound': lower_bounds,
        'upper_bound': upper_bounds
    }, index=column.index)
    df_bounds = df_bounds.replace([np.inf, -np.inf], np.nan)
    return df_bounds.tail(30)
    
def create_anomaly_chart(
    bounds_df: pd.DataFrame
    , metric_name: str
    ) -> go.Figure:
    """Create subplots for detected anomaly"""
    
    historical_series = bounds_df['value']
    current_value = bounds_df['value'].iloc[-1]
    lower_bounds = bounds_df['lower_bound']
    upper_bounds = bounds_df['upper_bound']
        
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=(
            f'Distribution',
            f'Historical Trend with Bounds'
        )
        , horizontal_spacing=0
        , column_widths=[0.4, 0.6]
        , shared_yaxes=True
    )
    
    fig.add_trace(
        go.Violin(
            y=historical_series.values,
            box_visible=True,
            meanline_visible=True,
            points=False,
            name=metric_name,
            marker=dict(color='gray'),
        ),
        row=1, col=1
    )

    fig.add_trace(
        go.Scatter(
            x=[metric_name], y=[current_value],
            mode='markers',
            marker=dict(color='red', size=12, symbol='x'),
            name='Current Value',
            showlegend=False
        ),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Scatter(
            x=historical_series.index,
            y=historical_series.values,
            mode='lines+markers',
            name='Historical data',
            line=dict(color='gray', shape='spline'),
            marker=dict(size=7)
        ),
        row=1, col=2
    )

    fig.add_trace(
        go.Scatter(
            x=upper_bounds.index,
            y=upper_bounds.values,
            mode='lines',
            name='Upper Bound',
            line=dict(color='gray', width=1, dash='dash'),
            showlegend=False
        ),
        row=1, col=2
    )
    
    fig.add_trace(
        go.Scatter(
            x=lower_bounds.index,
            y=lower_bounds.values,
            mode='lines',
            name='Lower Bound',
            line=dict(color='gray', width=1, dash='dash'),
            showlegend=False
        ),
        row=1, col=2
    )
    
    fig.add_trace(
        go.Scatter(
            x=[historical_series.index.max()],
            y=[current_value],
            mode='markers',
            marker=dict(color='red', size=10, symbol='x'),
            name='Current Value',
            showlegend=False
        ),
        row=1, col=2
    )

    fig.update_layout(
        title_text=f"Anomaly Detection: {metric_name}",
        height=450,
        width=1000,
        showlegend=False
    )
    
    fig.update_yaxes(title_text=metric_name, row=1, col=1)
    fig.update_yaxes(title_text=None, row=1, col=2)
    fig.update_xaxes(visible=False, row=1, col=1)
    fig.update_xaxes(title='Date', tickformat='%b %d', row=1, col=2)
    
    return fig    