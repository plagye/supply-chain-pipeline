import json
import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
from dotenv import load_dotenv
from urllib.parse import quote_plus

load_dotenv()
encoded_password = quote_plus(os.getenv('DB_PASSWORD'))
db_string = f"postgresql://{os.getenv('DB_USER')}:{encoded_password}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}?sslmode={os.getenv('DB_SSL')}"
engine = create_engine(db_string)

with engine.connect() as conn:
    result = conn.execute(text("SELECT MAX(timestamp) FROM fact_events"))
    max_ts = result.scalar()

def unpack_payload_and_load_staging(payload):
    return None