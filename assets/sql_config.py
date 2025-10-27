from dotenv import load_dotenv
from sqlalchemy import create_engine
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, '.env')

load_dotenv(env_path)

src_db_config = {
    'user': os.getenv('CLICKHOUSE_USER'),
    'pwd': os.getenv('CLICKHOUSE_PASSWORD'),
    'host': os.getenv('CLICKHOUSE_HOST'),
    'port': 9000, 
    'db': os.getenv('CLICKHOUSE_DATABASE'),
}

connection_string = (
    f"clickhouse+native://{src_db_config['user']}:{src_db_config['pwd']}"
    f"@{src_db_config['host']}:{src_db_config['port']}/{src_db_config['db']}"
)