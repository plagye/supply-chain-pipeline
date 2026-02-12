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

with open('../data/raw/inventory.json') as f:
    inventory_data = json.load(f)
df_inventory = pd.DataFrame.from_dict(inventory_data, orient='index').reset_index().rename(columns={'index': 'part_id'})

def load_suppliers():
    df_suppliers = pd.read_json('../data/raw/suppliers.json')
    df_suppliers = df_suppliers.rename(columns={'id': 'supplier_id'})
    df_suppliers.to_sql('dim_suppliers', engine, if_exists='append', index=False)
    return None

def load_customers():
    df_customers = pd.read_json('../data/raw/customers.json')
    df_customers.to_sql('dim_customers', engine, if_exists='append', index=False)
    return None

def load_parts():
    df_parts = pd.read_json('../data/raw/parts.json')
    df_parts = df_parts.merge(df_inventory[['part_id', 'reorder_point', 'safety_stock']], on='part_id', how='left')
    df_parts[['reorder_point', 'safety_stock']] = df_parts[['reorder_point', 'safety_stock']].fillna(0)
    df_parts = df_parts.drop(columns='valid_supplier_ids')
    df_parts.to_sql('dim_parts', engine, if_exists='append', index=False)
    return None

def load_facilities():
    df_facilities = pd.read_json('../data/raw/facilities.json')
    df_facilities.to_sql('dim_facilities', engine, if_exists='append', index=False)
    return None

def load_products():
    df_products = pd.read_json('../data/raw/products.json')
    df_products.to_sql('dim_products', engine, if_exists='append', index=False)
    return None

def load_routes():
    with open('../data/raw/routes.json', 'r') as f:
        data = json.load(f)
    all_routes = []

    for route in data.get('inbound', []):
        route['direction'] = 'inbound'
        all_routes.append(route)
    
    for route in data.get('outbound', []):
        route['direction'] = 'outbound'
        all_routes.append(route)
    
    df_routes = pd.DataFrame(all_routes)
    df_routes.to_sql('dim_routes', engine, if_exists='append', index=False)
    return None

def load_events():
    valid_records = []
    corrupted_records = []

    with open('../data/raw/events/history.jsonl', 'r') as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                valid_records.append(rec)
            except json.JSONDecodeError:
                pass     

    df_events = pd.DataFrame(valid_records)
    df_events["payload"] = df_events["payload"].apply(
        lambda x: json.dumps(x) if x is not None and isinstance(x, (dict, list)) else x
    )
    df_events.to_sql("fact_events", engine, if_exists="append", index=False)
    return None


if __name__ == "__main__":
    load_suppliers()
    load_customers()
    load_parts()
    load_events()
    load_facilities()
    load_products()
    load_routes()

