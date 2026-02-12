import os
import json
import pandas as pd
import paramiko
import re
from datetime import datetime
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy import text
from dotenv import load_dotenv
from urllib.parse import quote_plus

load_dotenv()
encoded_password = quote_plus(os.getenv('DB_PASSWORD'))
db_string = f"postgresql://{os.getenv('DB_USER')}:{encoded_password}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}?sslmode={os.getenv('DB_SSL')}"
engine = create_engine(db_string)

key_path = os.path.expanduser(os.getenv('SSH_KEY_PATH'))
pkey = paramiko.RSAKey.from_private_key_file(key_path)
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(
    hostname=os.getenv('REMOTE_HOST'),
    username=os.getenv('REMOTE_USER'),
    pkey=pkey,
)

sftp = client.open_sftp()
remote_files = sftp.listdir('/home/azureuser/supply-chain-simulator/data/events')
jsonl_files = [f for f in remote_files if f.endswith('.jsonl') and len(f) == 16]

with engine.connect() as conn:
    result = conn.execute(text("SELECT MAX(timestamp) FROM fact_events"))
    max_ts = result.scalar()

remote_events_path = '/home/azureuser/supply-chain-simulator/data/events'
min_date = max_ts.date() if max_ts else None

for f in jsonl_files:
    file_date_str = f.replace('.jsonl', '')
    try:
        file_date = datetime.strptime(file_date_str, '%Y-%m-%d').date()
    except ValueError:
        continue
    if min_date is not None and file_date < min_date:
        continue
    remote_path = f"{remote_events_path}/{f}"
    local_path = f"data/raw/events/{f}"
    sftp.get(remote_path, str(local_path))

sftp.close()
client.close()

DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}\.jsonl$")
file_paths = sorted(
    p for p in Path('data/raw/events').glob('*.jsonl')
    if DATE_PATTERN.match(p.name)
)

if not file_paths:
    print("No new events files found")
    exit(0)

def load_new_events(file_paths, max_ts=None):
    valid_records = []

    for path in file_paths:
        with open(path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    valid_records.append(rec)
                except json.JSONDecodeError:
                    pass

    if not valid_records:
        print("No new events found")
        return None

    df_events = pd.DataFrame(valid_records)
    df_events["timestamp"] = pd.to_datetime(df_events["timestamp"], utc=True, errors='coerce')
    df_events = df_events[~df_events["timestamp"].isna()]


    if max_ts is not None:
        df_events = df_events[df_events["timestamp"] > max_ts]
    if df_events.empty:
        print("No new events after filtering by timestamp")
        return None

    df_events["payload"] = df_events["payload"].apply(
        lambda x: json.dumps(x) if x is not None and isinstance(x, (dict, list)) else x
    )

    cols = ['timestamp', 'event_type', 'payload']
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            df_events[cols].to_sql("fact_events", conn, if_exists='append', index=False)
            trans.commit()
        except Exception:
            trans.rollback()
            raise

    return len(df_events)

if __name__ == "__main__":
    n = load_new_events(file_paths, max_ts=max_ts)
    if n is not None:
        print(f"Loaded {n} new events")
