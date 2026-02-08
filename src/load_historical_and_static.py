import os
import json
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from urllib.parse import quote_plus

load_dotenv()
encoded_password = quote_plus(os.getenv('DB_PASSWORD'))
db_string = f"postgresql://{os.getenv('DB_USER')}:{encoded_password}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}?sslmode={os.getenv('DB_SSL')}"
engine = create_engine(db_string)

inventory_data = json.load(open('../data/raw/inventory.json'))
df_inventory = pd.DataFrame.from_dict(inventory_data, orient='index').reset_index().rename(columns={'index': 'part_id'})

def load_suppliers():
    df_suppliers = pd.read_json('../data/raw/suppliers.json')
    df_suppliers = df_suppliers.rename(columns={'id': 'supplier_id'})
    df_suppliers.to_sql('dim_suppliers', engine, if_exists='append', index=False)
    return None

def load_customers():
    df_customers = pd.read_json('../data/raw/customers.json')
    # psycopg2 can't adapt dict; convert JSONB column to JSON string for insert
    df_customers["penalty_clauses"] = df_customers["penalty_clauses"].apply(
        lambda x: json.dumps(x) if x is not None and isinstance(x, (dict, list)) else x
    )
    df_customers.to_sql('dim_customers', engine, if_exists='append', index=False)
    return None

def load_parts():
    df_parts = pd.read_json('../data/raw/parts.json')
    df_parts = df_parts.merge(df_inventory[['part_id', 'reorder_point', 'safety_stock']], on='part_id', how='left')
    df_parts[['reorder_point', 'safety_stock']] = df_parts[['reorder_point', 'safety_stock']].fillna(0)
    df_inventory.drop(columns=['reorder_point', 'safety_stock'], inplace=True)
    df_parts = df_parts.drop(columns='valid_supplier_ids')
    df_parts.to_sql('dim_parts', engine, if_exists='append', index=False)
    return None

def load_inventory_snapshot():
    # fact_inventory_snapshots.part_id FK references dim_parts; inventory.json also has "DRONE-X1" (product),
    # which is not in dim_parts. Keep only rows whose part_id exists in dim_parts.
    valid_part_ids = pd.read_json('../data/raw/parts.json')['part_id']
    df_inventory_snapshot = df_inventory.copy()
    df_inventory_snapshot = df_inventory_snapshot[df_inventory_snapshot['part_id'].isin(valid_part_ids)]
    df_inventory_snapshot['timestamp'] = pd.Timestamp.now('UTC')
    df_inventory_snapshot.to_sql('fact_inventory_snapshots', engine, if_exists='append', index=False)
    return None

def load_events():
    valid_records = []
    corrupted_records = []

    with open('../data/raw/daily_events_log.jsonl', 'r') as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                if 'timestamp' in rec and 'event_type' in rec and 'payload' in rec:
                    valid_records.append(rec)
                else:
                    corrupted_records.append((line_no, "Missing required keys", line[:50]))
            except json.JSONDecodeError as e:
                corrupted_records.append((line_no, str(e), line[:100]))

    df_events = pd.DataFrame(valid_records)
    # Parse timestamps; drop rows with invalid/out-of-range values (e.g. "2026-01-01T25:00:00Z")
    df_events["timestamp"] = pd.to_datetime(df_events["timestamp"], utc=True, errors="coerce")
    df_events = df_events.dropna(subset=["timestamp"])
    # psycopg2 can't adapt dict; convert JSONB column to JSON string for insert
    df_events["payload"] = df_events["payload"].apply(
        lambda x: json.dumps(x) if x is not None and isinstance(x, (dict, list)) else x
    )
    df_events.to_sql('fact_events', engine, if_exists='append', index=False)
    return None


if __name__ == "__main__":
    load_suppliers()
    load_customers()
    load_parts()
    load_inventory_snapshot()
    load_events()

