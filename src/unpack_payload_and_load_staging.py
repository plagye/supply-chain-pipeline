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

query = """
        SELECT e.event_id, e.timestamp, e.event_type, e.payload
        FROM fact_events e
        WHERE e.event_id NOT IN (
            SELECT source_event_id FROM stg_orders
            UNION
            SELECT source_event_id FROM stg_loads
            UNION
            SELECT source_event_id FROM stg_backorders
            UNION
            SELECT source_event_id FROM stg_delivery_events
        )
        AND e.event_type IN (
            'SalesOrderCreated',
            'BackorderCreated',
            'LoadCreated',
            'DeliveryEvent'
        )
        ORDER BY e.event_id
        """

with engine.connect() as conn:
    df_events = pd.read_sql(text(query), conn)

if df_events.empty:
    print("No new events to unpack")
    exit(0)

df_sales = df_events[df_events['event_type'] == 'SalesOrderCreated'].reset_index(drop=True)
df_backorder = df_events[df_events['event_type'] == 'BackorderCreated'].reset_index(drop=True)
df_load = df_events[df_events['event_type'] == 'LoadCreated'].reset_index(drop=True)
df_delivery_events = df_events[df_events['event_type'] == 'DeliveryEvent'].reset_index(drop=True)

df_stg_orders = pd.DataFrame(df_sales['payload'].tolist())
df_stg_orders['source_event_id'] = df_sales['event_id'].values
df_stg_orders['order_date'] = df_sales['timestamp'].values

df_stg_backorders = pd.DataFrame(df_backorder['payload'].tolist())
df_stg_backorders['source_event_id'] = df_backorder['event_id'].values
df_stg_backorders['backorder_timestamp'] = df_backorder['timestamp'].values

df_stg_loads = pd.DataFrame(df_load['payload'].tolist())
df_stg_loads['source_event_id'] = df_load['event_id'].values

df_stg_delivery_events = pd.DataFrame(df_delivery_events['payload'].tolist())
df_stg_delivery_events['event_type'] = df_stg_delivery_events['event_type'].map({'Pickup': 'P', 'Delivery': 'D'})
df_stg_delivery_events['source_event_id'] = df_delivery_events['event_id'].values
df_stg_delivery_events['event_timestamp'] = df_delivery_events['timestamp'].values

with engine.connect() as conn:
    trans = conn.begin()
    try:
        df_stg_orders.to_sql('stg_orders', conn, if_exists='append', index=False)
        df_stg_backorders.to_sql('stg_backorders', conn, if_exists='append', index=False)
        df_stg_loads.to_sql('stg_loads', conn, if_exists='append', index=False)
        df_stg_delivery_events.to_sql('stg_delivery_events', conn, if_exists='append', index=False)
        trans.commit()
        print(f'Inserted {len(df_stg_orders)} rows into stg_orders')
        print(f'Inserted {len(df_stg_backorders)} rows into stg_backorders')
        print(f'Inserted {len(df_stg_loads)} rows into stg_loads')
        print(f'Inserted {len(df_stg_delivery_events)} rows into stg_delivery_events')
    except Exception:
        trans.rollback()
        raise