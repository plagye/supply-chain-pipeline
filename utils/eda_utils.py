import os
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy import text
from dotenv import load_dotenv
from urllib.parse import quote_plus

load_dotenv()

def get_engine():
    encoded_password = quote_plus(os.getenv('DB_PASSWORD'))
    db_string = f"postgresql://{os.getenv('DB_USER')}:{encoded_password}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}?sslmode={os.getenv('DB_SSL')}"
    engine = create_engine(db_string)
    return engine

def run_query(sql, params=None):
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params)
    return df

def date_range(start, end=None):
    if end:
        return {
            'start': start,
            'end': end
        }
    elif end is None:
        return {
            'start': start,
            'end': datetime.now().date()
        }
    else:
        raise ValueError("Invalid date range")