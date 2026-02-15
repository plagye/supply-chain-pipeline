import os
import json
import pandas as pd
from sqlalchemy import ARRAY, UUID
from sqlalchemy import create_engine
from sqlalchemy import text
from dotenv import load_dotenv
from urllib.parse import quote_plus

load_dotenv()
encoded_password = quote_plus(os.getenv('DB_PASSWORD'))
db_string = f"postgresql://{os.getenv('DB_USER')}:{encoded_password}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}?sslmode={os.getenv('DB_SSL')}"
engine = create_engine(db_string)

dtype_mapping = {
    'order_ids': ARRAY(UUID(as_uuid=True))
}

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
            UNION
            SELECT source_event_id FROM stg_invoices
            UNION
            SELECT source_event_id FROM stg_demand_forecasts
            UNION
            SELECT source_event_id FROM stg_production_jobs
            UNION
            SELECT source_event_id FROM stg_purchase_orders
            UNION
            SELECT source_event_id FROM stg_po_receipts
            UNION
            SELECT source_event_id FROM stg_backorder_fulfillments
            UNION
            SELECT source_event_id FROM stg_shipments
            UNION
            SELECT source_event_id FROM stg_material_requirements
            UNION
            SELECT source_event_id FROM stg_production_starts
            UNION
            SELECT source_event_id FROM stg_production_completions
            UNION
            SELECT source_event_id FROM stg_sop_snapshots
        )
        AND e.event_type IN (
            'SalesOrderCreated',
            'BackorderCreated',
            'LoadCreated',
            'DeliveryEvent',
            'InvoiceCreated',
            'DemandForecastCreated',
            'ProductionJobCreated',
            'PurchaseOrderCreated',
            'PurchaseOrderReceived',
            'BackorderFulfilled',
            'ShipmentCreated',
            'MaterialRequirementsCreated',
            'ProductionStarted',
            'ProductionCompleted',
            'SOPSnapshotCreated'
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
df_invoices = df_events[df_events['event_type'] == 'InvoiceCreated'].reset_index(drop=True)
df_demand_forecasts = df_events[df_events['event_type'] == 'DemandForecastCreated'].reset_index(drop=True)
df_production_jobs = df_events[df_events['event_type'] == 'ProductionJobCreated'].reset_index(drop=True)
df_purchase_orders = df_events[df_events['event_type'] == 'PurchaseOrderCreated'].reset_index(drop=True)
df_po_receipts = df_events[df_events['event_type'] == 'PurchaseOrderReceived'].reset_index(drop=True)
df_backorder_fulfillments = df_events[df_events['event_type'] == 'BackorderFulfilled'].reset_index(drop=True)
df_shipments = df_events[df_events['event_type'] == 'ShipmentCreated'].reset_index(drop=True)
df_material_requirements = df_events[df_events['event_type'] == 'MaterialRequirementsCreated'].reset_index(drop=True)
df_production_starts = df_events[df_events['event_type'] == 'ProductionStarted'].reset_index(drop=True)
df_production_completions = df_events[df_events['event_type'] == 'ProductionCompleted'].reset_index(drop=True)
df_sop_snapshots = df_events[df_events['event_type'] == 'SOPSnapshotCreated'].reset_index(drop=True)

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

df_stg_invoices = pd.DataFrame(df_invoices['payload'].tolist())
df_stg_invoices['source_event_id'] = df_invoices['event_id'].values
df_stg_invoices['invoice_timestamp'] = df_invoices['timestamp'].values
df_stg_invoices = df_stg_invoices.drop(columns=['timestamp'], errors='ignore')

df_stg_demand_forecasts = pd.DataFrame(df_demand_forecasts['payload'].tolist())
df_stg_demand_forecasts['source_event_id'] = df_demand_forecasts['event_id'].values
df_stg_demand_forecasts['event_timestamp'] = df_demand_forecasts['timestamp'].values

df_stg_production_jobs = pd.DataFrame(df_production_jobs['payload'].tolist())
df_stg_production_jobs['source_event_id'] = df_production_jobs['event_id'].values
df_stg_production_jobs['event_timestamp'] = df_production_jobs['timestamp'].values

df_stg_purchase_orders = pd.DataFrame(df_purchase_orders['payload'].tolist())
df_stg_purchase_orders['source_event_id'] = df_purchase_orders['event_id'].values
df_stg_purchase_orders['event_timestamp'] = df_purchase_orders['timestamp'].values
df_stg_purchase_orders = df_stg_purchase_orders.drop(columns=[
    'cost_variance_pct',
    'supplier_reliability',
    'effective_reliability',
    'seasonal_lead_time_mult',
    'seasonal_reliability_mult'
], errors='ignore')

df_stg_po_receipts = pd.DataFrame(df_po_receipts['payload'].tolist())
df_stg_po_receipts['source_event_id'] = df_po_receipts['event_id'].values
df_stg_po_receipts['received_timestamp'] = df_po_receipts['timestamp'].values

df_stg_backorder_fulfillments = pd.DataFrame(df_backorder_fulfillments['payload'].tolist())
df_stg_backorder_fulfillments['source_event_id'] = df_backorder_fulfillments['event_id'].values
df_stg_backorder_fulfillments['event_timestamp'] = df_backorder_fulfillments['timestamp'].values

df_stg_shipments = pd.DataFrame(df_shipments['payload'].tolist())
df_stg_shipments['source_event_id'] = df_shipments['event_id'].values
df_stg_shipments['event_timestamp'] = df_shipments['timestamp'].values

df_stg_material_requirements = pd.DataFrame(df_material_requirements['payload'].tolist())
df_stg_material_requirements['source_event_id'] = df_material_requirements['event_id'].values
df_stg_material_requirements['event_timestamp'] = df_material_requirements['timestamp'].values
df_stg_material_requirements['requirements'] = df_stg_material_requirements['requirements'].apply(
    lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x
)

df_stg_production_starts = pd.DataFrame(df_production_starts['payload'].tolist())
df_stg_production_starts['source_event_id'] = df_production_starts['event_id'].values
df_stg_production_starts['event_timestamp'] = df_production_starts['timestamp'].values

df_stg_production_completions = pd.DataFrame(df_production_completions['payload'].tolist())
df_stg_production_completions['source_event_id'] = df_production_completions['event_id'].values
df_stg_production_completions['event_timestamp'] = df_production_completions['timestamp'].values

df_stg_sop_snapshots = pd.DataFrame(df_sop_snapshots['payload'].tolist())
df_stg_sop_snapshots['source_event_id'] = df_sop_snapshots['event_id'].values
df_stg_sop_snapshots['event_timestamp'] = df_sop_snapshots['timestamp'].values
with engine.connect() as conn:
    trans = conn.begin()
    try:
        dfs = [
            df_stg_orders,
            df_stg_backorders,
            df_stg_loads,
            df_stg_delivery_events,
            df_stg_invoices,
            df_stg_demand_forecasts,
            df_stg_production_jobs,
            df_stg_purchase_orders,
            df_stg_po_receipts,
            df_stg_backorder_fulfillments,
            df_stg_shipments,
            df_stg_material_requirements,
            df_stg_production_starts,
            df_stg_production_completions,
            df_stg_sop_snapshots
        ]
        tables = [
            'stg_orders',
            'stg_backorders',
            'stg_loads',
            'stg_delivery_events',
            'stg_invoices',
            'stg_demand_forecasts',
            'stg_production_jobs',
            'stg_purchase_orders',
            'stg_po_receipts',
            'stg_backorder_fulfillments',
            'stg_shipments',
            'stg_material_requirements',
            'stg_production_starts',
            'stg_production_completions',
            'stg_sop_snapshots'
        ]
        for df, table in zip(dfs, tables):
            if table == 'stg_sop_snapshots':
                df = df[~df['product_id'].str.startswith('P-')]
                print(f"Filtered {len(df)} parts from {table}")
            df.to_sql(table, conn, if_exists='append', index=False, dtype=dtype_mapping)
        trans.commit()
        for df, table in zip(dfs, tables):
            print(f'Inserted {len(df)} rows into {table}')
    except Exception:
        trans.rollback()
        raise
