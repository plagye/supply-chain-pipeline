"""
Create and refresh BI/data mart tables from staging and dimensions.
Run after init_db and after staging is populated.
"""
import os
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_NAME"),
}
encoded_password = quote_plus(DB_CONFIG["password"])
SSL_ARGS = {"sslmode": os.getenv("DB_SSL", "require")}


def get_engine():
    conn_str = (
        f"postgresql://{DB_CONFIG['user']}:{encoded_password}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(conn_str, connect_args=SSL_ARGS)


# ---------------------------------------------------------------------------
# 1. Sales & revenue mart (invoice line + payment summary)
# ---------------------------------------------------------------------------
DDL_MART_SALES_REVENUE = """
CREATE TABLE IF NOT EXISTS mart_sales_revenue (
    invoice_id UUID NOT NULL,
    order_id UUID NOT NULL,
    customer_id VARCHAR(100),
    customer_name VARCHAR(255),
    customer_segment VARCHAR(100),
    customer_region VARCHAR(100),
    product_id VARCHAR(100),
    product_name VARCHAR(255),
    product_type VARCHAR(100),
    order_date TIMESTAMPTZ,
    invoice_date TIMESTAMPTZ,
    due_date TIMESTAMPTZ,
    paid_at TIMESTAMPTZ,
    qty INTEGER,
    unit_price DECIMAL(10,2),
    amount DECIMAL(12,2),
    amount_paid DECIMAL(12,2),
    currency VARCHAR(10),
    on_time_payment BOOLEAN,
    source_event_id BIGINT,
    PRIMARY KEY (invoice_id)
);
"""

REFRESH_MART_SALES_REVENUE = """
INSERT INTO mart_sales_revenue (
    invoice_id, order_id, customer_id, customer_name, customer_segment, customer_region,
    product_id, product_name, product_type, order_date, invoice_date, due_date, paid_at,
    qty, unit_price, amount, amount_paid, currency, on_time_payment, source_event_id
)
SELECT
    i.invoice_id,
    i.order_id,
    i.customer_id,
    c.company_name AS customer_name,
    c.segment AS customer_segment,
    c.region AS customer_region,
    i.product_id,
    p.name AS product_name,
    p.type AS product_type,
    o.order_date AS order_date,
    i.invoice_timestamp AS invoice_date,
    i.due_date,
    pay.paid_at,
    i.qty,
    o.unit_price,
    i.amount,
    COALESCE(pay.amount_paid, 0) AS amount_paid,
    i.currency,
    pay.on_time_payment,
    i.source_event_id
FROM stg_invoices i
JOIN stg_orders o ON o.order_id = i.order_id
LEFT JOIN dim_customers c ON c.customer_id = i.customer_id
LEFT JOIN dim_products p ON p.product_id = i.product_id
LEFT JOIN (
    SELECT
        invoice_id,
        SUM(amount) AS amount_paid,
        MAX(paid_at) AS paid_at,
        BOOL_AND(on_time) AS on_time_payment
    FROM stg_payments
    GROUP BY invoice_id
) pay ON pay.invoice_id = i.invoice_id
ON CONFLICT (invoice_id) DO UPDATE SET
    amount_paid = EXCLUDED.amount_paid,
    paid_at = EXCLUDED.paid_at,
    on_time_payment = EXCLUDED.on_time_payment;
"""

# Insert-only (used with separate TRUNCATE so both run)
INSERT_MART_SALES_REVENUE = """
INSERT INTO mart_sales_revenue (
    invoice_id, order_id, customer_id, customer_name, customer_segment, customer_region,
    product_id, product_name, product_type, order_date, invoice_date, due_date, paid_at,
    qty, unit_price, amount, amount_paid, currency, on_time_payment, source_event_id
)
SELECT
    i.invoice_id, i.order_id, i.customer_id,
    c.company_name, c.segment, c.region,
    i.product_id, p.name, p.type,
    o.order_date, i.invoice_timestamp, i.due_date, pay.paid_at,
    i.qty, o.unit_price, i.amount, COALESCE(pay.amount_paid, 0), i.currency, pay.on_time_payment,
    i.source_event_id
FROM stg_invoices i
JOIN stg_orders o ON o.order_id = i.order_id
LEFT JOIN dim_customers c ON c.customer_id = i.customer_id
LEFT JOIN dim_products p ON p.product_id = i.product_id
LEFT JOIN (
    SELECT invoice_id, SUM(amount) AS amount_paid, MAX(paid_at) AS paid_at, BOOL_AND(on_time) AS on_time_payment
    FROM stg_payments GROUP BY invoice_id
) pay ON pay.invoice_id = i.invoice_id
"""

# ---------------------------------------------------------------------------
# 2. Orders & fulfillment mart (one row per order with backorder/ship summary)
# ---------------------------------------------------------------------------
DDL_MART_ORDERS_FULFILLMENT = """
CREATE TABLE IF NOT EXISTS mart_orders_fulfillment (
    order_id UUID PRIMARY KEY,
    customer_id VARCHAR(100),
    customer_name VARCHAR(255),
    customer_region VARCHAR(100),
    product_id VARCHAR(100),
    product_name VARCHAR(255),
    order_date TIMESTAMPTZ,
    order_qty INTEGER,
    unit_price DECIMAL(10,2),
    line_total DECIMAL(12,2),
    backorder_qty INTEGER,
    backorder_timestamp TIMESTAMPTZ,
    shipped_qty INTEGER,
    ship_date TIMESTAMPTZ,
    fulfillment_amount DECIMAL(12,2)
);
"""

INSERT_MART_ORDERS_FULFILLMENT = """
INSERT INTO mart_orders_fulfillment (
    order_id, customer_id, customer_name, customer_region, product_id, product_name,
    order_date, order_qty, unit_price, line_total, backorder_qty, backorder_timestamp,
    shipped_qty, ship_date, fulfillment_amount
)
SELECT
    o.order_id,
    o.customer_id,
    c.company_name AS customer_name,
    c.region AS customer_region,
    o.product_id,
    p.name AS product_name,
    o.order_date,
    o.qty AS order_qty,
    o.unit_price,
    o.line_total,
    bo.qty_backordered AS backorder_qty,
    bo.backorder_timestamp,
    COALESCE(sh.shipped_qty, 0) AS shipped_qty,
    sh.ship_date,
    COALESCE(sh.fulfillment_amount, 0) AS fulfillment_amount
FROM stg_orders o
LEFT JOIN dim_customers c ON c.customer_id = o.customer_id
LEFT JOIN dim_products p ON p.product_id = o.product_id
LEFT JOIN stg_backorders bo ON bo.order_id = o.order_id
LEFT JOIN (
    SELECT
        order_id,
        SUM(qty) AS shipped_qty,
        MIN(event_timestamp) AS ship_date,
        SUM(amount) AS fulfillment_amount
    FROM stg_shipments
    GROUP BY order_id
) sh ON sh.order_id = o.order_id
"""

# ---------------------------------------------------------------------------
# 3. Logistics / delivery mart (one row per load + delivery outcome)
# ---------------------------------------------------------------------------
DDL_MART_LOGISTICS = """
CREATE TABLE IF NOT EXISTS mart_logistics (
    load_id UUID PRIMARY KEY,
    order_id UUID,
    route_id VARCHAR(100),
    origin_facility_id VARCHAR(100),
    destination_facility_id VARCHAR(100),
    customer_id VARCHAR(100),
    product_id VARCHAR(100),
    product_name VARCHAR(255),
    qty INTEGER,
    distance_miles INTEGER,
    load_status VARCHAR(50),
    scheduled_pickup TIMESTAMPTZ,
    scheduled_delivery TIMESTAMPTZ,
    actual_delivery TIMESTAMPTZ,
    delivery_on_time BOOLEAN,
    created_at TIMESTAMPTZ
);
"""

INSERT_MART_LOGISTICS = """
INSERT INTO mart_logistics (
    load_id, order_id, route_id, origin_facility_id, destination_facility_id,
    customer_id, product_id, product_name, qty, distance_miles, load_status,
    scheduled_pickup, scheduled_delivery, actual_delivery, delivery_on_time, created_at
)
SELECT
    l.load_id,
    l.order_id,
    l.route_id,
    r.origin_facility_id,
    r.destination_facility_id,
    l.customer_id,
    l.product_id,
    p.name AS product_name,
    l.qty,
    l.distance_miles,
    l.load_status,
    l.scheduled_pickup,
    l.scheduled_delivery,
    l.actual_delivery,
    de.on_time_flag AS delivery_on_time,
    l.created_at
FROM stg_loads l
LEFT JOIN dim_routes r ON r.route_id = l.route_id
LEFT JOIN dim_products p ON p.product_id = l.product_id
LEFT JOIN LATERAL (
    SELECT on_time_flag
    FROM stg_delivery_events
    WHERE load_id = l.load_id AND event_type = 'D'
    LIMIT 1
) de ON true
"""

# ---------------------------------------------------------------------------
# 4. Procurement mart (one row per PO with receipt summary)
# ---------------------------------------------------------------------------
DDL_MART_PROCUREMENT = """
CREATE TABLE IF NOT EXISTS mart_procurement (
    purchase_order_id UUID PRIMARY KEY,
    part_id VARCHAR(50),
    part_name VARCHAR(255),
    supplier_id VARCHAR(100),
    supplier_name VARCHAR(255),
    supplier_country VARCHAR(100),
    order_date TIMESTAMPTZ,
    qty INTEGER,
    unit_cost DECIMAL(12,2),
    total_cost DECIMAL(14,2),
    lead_time_hours INTEGER,
    eta TIMESTAMPTZ,
    is_reorder BOOLEAN,
    qty_received INTEGER,
    qty_rejected INTEGER,
    actual_receipt_time TIMESTAMPTZ
);
"""

INSERT_MART_PROCUREMENT = """
INSERT INTO mart_procurement (
    purchase_order_id, part_id, part_name, supplier_id, supplier_name, supplier_country,
    order_date, qty, unit_cost, total_cost, lead_time_hours, eta, is_reorder,
    qty_received, qty_rejected, actual_receipt_time
)
SELECT
    po.purchase_order_id,
    po.part_id,
    pt.name AS part_name,
    po.supplier_id,
    s.name AS supplier_name,
    COALESCE(po.supplier_country, s.country) AS supplier_country,
    po.event_timestamp AS order_date,
    po.qty,
    po.unit_cost,
    po.total_cost,
    po.lead_time_hours,
    po.eta,
    po.is_reorder,
    COALESCE(rcpt.qty_received, 0) AS qty_received,
    COALESCE(rcpt.qty_rejected, 0) AS qty_rejected,
    rcpt.actual_receipt_time
FROM stg_purchase_orders po
LEFT JOIN dim_parts pt ON pt.part_id = po.part_id
LEFT JOIN dim_suppliers s ON s.supplier_id = po.supplier_id
LEFT JOIN (
    SELECT
        purchase_order_id,
        SUM(qty_received) AS qty_received,
        SUM(qty_rejected) AS qty_rejected,
        MAX(received_timestamp) AS actual_receipt_time
    FROM stg_po_receipts
    GROUP BY purchase_order_id
) rcpt ON rcpt.purchase_order_id = po.purchase_order_id
"""

# ---------------------------------------------------------------------------
# 5. Production mart (one row per job with start/completion)
# ---------------------------------------------------------------------------
DDL_MART_PRODUCTION = """
CREATE TABLE IF NOT EXISTS mart_production (
    job_id UUID PRIMARY KEY,
    product_id VARCHAR(100),
    product_name VARCHAR(255),
    status VARCHAR(50),
    qty_per_job INTEGER,
    qty_produced INTEGER,
    start_timestamp TIMESTAMPTZ,
    completion_timestamp TIMESTAMPTZ,
    production_duration_hours INTEGER,
    new_qty_on_hand INTEGER
);
"""

INSERT_MART_PRODUCTION = """
INSERT INTO mart_production (
    job_id, product_id, product_name, status, qty_per_job, qty_produced,
    start_timestamp, completion_timestamp, production_duration_hours, new_qty_on_hand
)
SELECT
    j.job_id,
    j.product_id,
    p.name AS product_name,
    COALESCE(pc.status, ps.status, j.status) AS status,
    j.qty_per_job,
    pc.qty_produced,
    ps.event_timestamp AS start_timestamp,
    pc.event_timestamp AS completion_timestamp,
    j.production_duration_hours,
    pc.new_qty_on_hand
FROM stg_production_jobs j
LEFT JOIN dim_products p ON p.product_id = j.product_id
LEFT JOIN stg_production_starts ps ON ps.job_id = j.job_id
LEFT JOIN stg_production_completions pc ON pc.job_id = j.job_id
"""

# ---------------------------------------------------------------------------
# 7a. Demand forecasts mart
# ---------------------------------------------------------------------------
DDL_MART_DEMAND_FORECASTS = """
CREATE TABLE IF NOT EXISTS mart_demand_forecasts (
    source_event_id BIGINT PRIMARY KEY,
    product_id VARCHAR(100),
    product_name VARCHAR(255),
    snapshot_date DATE,
    forecast_date DATE,
    horizon_days INTEGER,
    forecast_qty DECIMAL(12,2),
    event_timestamp TIMESTAMPTZ
);
"""

INSERT_MART_DEMAND_FORECASTS = """
INSERT INTO mart_demand_forecasts (
    source_event_id, product_id, product_name, snapshot_date, forecast_date,
    horizon_days, forecast_qty, event_timestamp
)
SELECT
    df.source_event_id,
    df.product_id,
    p.name AS product_name,
    df.snapshot_date,
    df.forecast_date,
    df.horizon_days,
    df.forecast_qty,
    df.event_timestamp
FROM stg_demand_forecasts df
LEFT JOIN dim_products p ON p.product_id = df.product_id
"""

# ---------------------------------------------------------------------------
# 7b. S&OP snapshots mart
# ---------------------------------------------------------------------------
DDL_MART_SOP_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS mart_sop_snapshots (
    source_event_id BIGINT PRIMARY KEY,
    plan_date DATE NOT NULL,
    scenario VARCHAR(100),
    product_id VARCHAR(100),
    product_name VARCHAR(255),
    demand_forecast_qty DECIMAL(12,2),
    supply_plan_qty DECIMAL(12,2),
    inventory_plan_qty DECIMAL(12,2),
    event_timestamp TIMESTAMPTZ NOT NULL
);
"""

INSERT_MART_SOP_SNAPSHOTS = """
INSERT INTO mart_sop_snapshots (
    source_event_id, plan_date, scenario, product_id, product_name,
    demand_forecast_qty, supply_plan_qty, inventory_plan_qty, event_timestamp
)
SELECT
    ss.source_event_id,
    ss.plan_date,
    ss.scenario,
    ss.product_id,
    p.name AS product_name,
    ss.demand_forecast_qty,
    ss.supply_plan_qty,
    ss.inventory_plan_qty,
    ss.event_timestamp
FROM stg_sop_snapshots ss
LEFT JOIN dim_products p ON p.product_id = ss.product_id
"""

# ---------------------------------------------------------------------------
# Orders per customer per month (for fast Excel: filter year/month, get results in seconds)
# ---------------------------------------------------------------------------
DDL_MART_ORDERS_BY_CUSTOMER_MONTH = """
CREATE TABLE IF NOT EXISTS mart_orders_by_customer_month (
    customer_id VARCHAR(100) NOT NULL,
    customer_name VARCHAR(255),
    customer_region VARCHAR(100),
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_start DATE NOT NULL,
    order_count INTEGER NOT NULL,
    total_qty INTEGER NOT NULL,
    total_amount DECIMAL(14,2) NOT NULL,
    PRIMARY KEY (customer_id, year, month)
)
"""
DDL_MART_ORDERS_BY_CUSTOMER_MONTH_IX = """
CREATE INDEX IF NOT EXISTS ix_mart_orders_by_customer_month_month_start
    ON mart_orders_by_customer_month (month_start)
"""
DDL_MART_ORDERS_BY_CUSTOMER_MONTH_IX2 = """
CREATE INDEX IF NOT EXISTS ix_mart_orders_by_customer_month_year_month
    ON mart_orders_by_customer_month (year, month)
"""

INSERT_MART_ORDERS_BY_CUSTOMER_MONTH = """
INSERT INTO mart_orders_by_customer_month (
    customer_id, customer_name, customer_region, year, month, month_start,
    order_count, total_qty, total_amount
)
SELECT
    o.customer_id,
    c.company_name AS customer_name,
    c.region AS customer_region,
    EXTRACT(YEAR FROM o.order_date)::INTEGER AS year,
    EXTRACT(MONTH FROM o.order_date)::INTEGER AS month,
    DATE_TRUNC('month', o.order_date AT TIME ZONE 'UTC')::DATE AS month_start,
    COUNT(*)::INTEGER AS order_count,
    COALESCE(SUM(o.qty), 0)::INTEGER AS total_qty,
    COALESCE(SUM(o.line_total), 0) AS total_amount
FROM stg_orders o
LEFT JOIN dim_customers c ON c.customer_id = o.customer_id
WHERE o.order_date IS NOT NULL
GROUP BY o.customer_id, c.company_name, c.region,
         EXTRACT(YEAR FROM o.order_date),
         EXTRACT(MONTH FROM o.order_date),
         DATE_TRUNC('month', o.order_date AT TIME ZONE 'UTC')
"""

# ---------------------------------------------------------------------------
# Forecast vs actual orders (weekly and monthly, for Excel comparison)
# ---------------------------------------------------------------------------
DDL_MART_FORECAST_VS_ACTUAL = """
CREATE TABLE IF NOT EXISTS mart_forecast_vs_actual (
    product_id VARCHAR(100) NOT NULL,
    product_name VARCHAR(255),
    period_type VARCHAR(10) NOT NULL,
    period_start DATE NOT NULL,
    forecast_qty DECIMAL(14,2) NOT NULL DEFAULT 0,
    actual_qty DECIMAL(14,2) NOT NULL DEFAULT 0,
    variance_qty DECIMAL(14,2),
    variance_pct DECIMAL(8,2),
    actual_amount DECIMAL(14,2) NOT NULL DEFAULT 0,
    PRIMARY KEY (product_id, period_type, period_start)
)
"""
DDL_MART_FORECAST_VS_ACTUAL_IX = """
CREATE INDEX IF NOT EXISTS ix_mart_forecast_vs_actual_period
    ON mart_forecast_vs_actual (period_type, period_start)
"""

INSERT_MART_FORECAST_VS_ACTUAL = """
WITH
forecast_week AS (
    SELECT product_id,
           (DATE_TRUNC('week', (forecast_date::timestamp AT TIME ZONE 'UTC')))::date AS period_start,
           SUM(forecast_qty) AS forecast_qty
    FROM stg_demand_forecasts
    WHERE forecast_date IS NOT NULL
    GROUP BY product_id, DATE_TRUNC('week', (forecast_date::timestamp AT TIME ZONE 'UTC'))
),
forecast_month AS (
    SELECT product_id,
           (DATE_TRUNC('month', (forecast_date::timestamp AT TIME ZONE 'UTC')))::date AS period_start,
           SUM(forecast_qty) AS forecast_qty
    FROM stg_demand_forecasts
    WHERE forecast_date IS NOT NULL
    GROUP BY product_id, DATE_TRUNC('month', (forecast_date::timestamp AT TIME ZONE 'UTC'))
),
actual_week AS (
    SELECT product_id,
           (DATE_TRUNC('week', order_date AT TIME ZONE 'UTC'))::date AS period_start,
           SUM(qty) AS actual_qty,
           SUM(line_total) AS actual_amount
    FROM stg_orders
    WHERE order_date IS NOT NULL
    GROUP BY product_id, DATE_TRUNC('week', order_date AT TIME ZONE 'UTC')
),
actual_month AS (
    SELECT product_id,
           (DATE_TRUNC('month', order_date AT TIME ZONE 'UTC'))::date AS period_start,
           SUM(qty) AS actual_qty,
           SUM(line_total) AS actual_amount
    FROM stg_orders
    WHERE order_date IS NOT NULL
    GROUP BY product_id, DATE_TRUNC('month', order_date AT TIME ZONE 'UTC')
),
combined_week AS (
    SELECT 'week'::varchar(10) AS period_type,
           COALESCE(f.product_id, a.product_id) AS product_id,
           COALESCE(f.period_start, a.period_start) AS period_start,
           COALESCE(f.forecast_qty, 0) AS forecast_qty,
           COALESCE(a.actual_qty, 0) AS actual_qty,
           COALESCE(a.actual_amount, 0) AS actual_amount
    FROM forecast_week f
    FULL OUTER JOIN actual_week a ON f.product_id = a.product_id AND f.period_start = a.period_start
),
combined_month AS (
    SELECT 'month'::varchar(10) AS period_type,
           COALESCE(f.product_id, a.product_id) AS product_id,
           COALESCE(f.period_start, a.period_start) AS period_start,
           COALESCE(f.forecast_qty, 0) AS forecast_qty,
           COALESCE(a.actual_qty, 0) AS actual_qty,
           COALESCE(a.actual_amount, 0) AS actual_amount
    FROM forecast_month f
    FULL OUTER JOIN actual_month a ON f.product_id = a.product_id AND f.period_start = a.period_start
),
all_periods AS (
    SELECT period_type, product_id, period_start, forecast_qty, actual_qty, actual_amount FROM combined_week
    UNION ALL
    SELECT period_type, product_id, period_start, forecast_qty, actual_qty, actual_amount FROM combined_month
)
INSERT INTO mart_forecast_vs_actual (product_id, product_name, period_type, period_start, forecast_qty, actual_qty, variance_qty, variance_pct, actual_amount)
SELECT
    a.product_id,
    p.name AS product_name,
    a.period_type,
    a.period_start,
    a.forecast_qty,
    a.actual_qty,
    (a.actual_qty - a.forecast_qty) AS variance_qty,
    CASE WHEN a.forecast_qty <> 0 THEN ROUND(((a.actual_qty - a.forecast_qty) / a.forecast_qty * 100)::numeric, 2) ELSE NULL END AS variance_pct,
    a.actual_amount
FROM all_periods a
LEFT JOIN dim_products p ON p.product_id = a.product_id
"""

# Table name + INSERT SQL for refresh (TRUNCATE run separately so both execute)
_REFRESH_LIST = [
    ("mart_sales_revenue", INSERT_MART_SALES_REVENUE),
    ("mart_orders_fulfillment", INSERT_MART_ORDERS_FULFILLMENT),
    ("mart_logistics", INSERT_MART_LOGISTICS),
    ("mart_procurement", INSERT_MART_PROCUREMENT),
    ("mart_production", INSERT_MART_PRODUCTION),
    ("mart_demand_forecasts", INSERT_MART_DEMAND_FORECASTS),
    ("mart_sop_snapshots", INSERT_MART_SOP_SNAPSHOTS),
    ("mart_orders_by_customer_month", INSERT_MART_ORDERS_BY_CUSTOMER_MONTH),
    ("mart_forecast_vs_actual", INSERT_MART_FORECAST_VS_ACTUAL),
]


def init_marts():
    """Create mart tables if they do not exist."""
    engine = get_engine()
    ddl_list = [
        DDL_MART_SALES_REVENUE,
        DDL_MART_ORDERS_FULFILLMENT,
        DDL_MART_LOGISTICS,
        DDL_MART_PROCUREMENT,
        DDL_MART_PRODUCTION,
        DDL_MART_DEMAND_FORECASTS,
        DDL_MART_SOP_SNAPSHOTS,
        DDL_MART_ORDERS_BY_CUSTOMER_MONTH,
        DDL_MART_ORDERS_BY_CUSTOMER_MONTH_IX,
        DDL_MART_ORDERS_BY_CUSTOMER_MONTH_IX2,
        DDL_MART_FORECAST_VS_ACTUAL,
        DDL_MART_FORECAST_VS_ACTUAL_IX,
    ]
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            for ddl in ddl_list:
                conn.execute(text(ddl))
            trans.commit()
            print("SUCCESS: Mart tables created.")
        except Exception as e:
            trans.rollback()
            raise RuntimeError(f"Error creating mart tables: {e}") from e
        finally:
            engine.dispose()


def refresh_marts():
    """Full refresh: truncate and repopulate all marts from staging/dims.
    TRUNCATE and INSERT are run as separate statements so both execute (some drivers
    only run the first statement in a multi-statement string).
    """
    engine = get_engine()
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            for table_name, insert_sql in _REFRESH_LIST:
                conn.execute(text(f"TRUNCATE {table_name}"))
                conn.execute(text(insert_sql))
            trans.commit()
            print("SUCCESS: Marts refreshed.")
        except Exception as e:
            trans.rollback()
            raise RuntimeError(f"Error refreshing marts: {e}") from e
        finally:
            engine.dispose()


if __name__ == "__main__":
    init_marts()
    refresh_marts()
