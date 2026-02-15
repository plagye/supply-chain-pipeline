import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv
import os
import urllib.parse

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_NAME"),
}

encoded_password = urllib.parse.quote_plus(DB_CONFIG["password"])

SSL_ARGS = {"sslmode": "require"}

def get_engine(db_name):
    conn_str = f"postgresql://{DB_CONFIG['user']}:{encoded_password}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{db_name}"
    engine = create_engine(conn_str, connect_args=SSL_ARGS)
    return engine

def init_schema():
    engine = get_engine("postgres")

    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f"CREATE DATABASE {DB_CONFIG['database']}"))

    engine.dispose()

def init_tables():
    engine = get_engine(DB_CONFIG['database'])

    ddl_statements = [
        # DIM
        """
            CREATE TABLE IF NOT EXISTS dim_suppliers (
            supplier_id VARCHAR(100) PRIMARY KEY,
            name VARCHAR(255),
            country VARCHAR(100),
            reliability_score DECIMAL(3,2),
            risk_factor VARCHAR(50),
            price_multiplier DECIMAL(4,2)
        );
        """,
        """
            CREATE TABLE IF NOT EXISTS dim_customers (
            customer_id VARCHAR(100) PRIMARY KEY,
            company_name VARCHAR(255),
            segment VARCHAR(100),
            region VARCHAR(100),
            country VARCHAR(100),
            street VARCHAR(255),
            city VARCHAR(100),
            state VARCHAR(100),
            postal_code VARCHAR(50),
            destination_facility_id VARCHAR(100),
            delivery_location_code VARCHAR(100),
            contract_priority VARCHAR(50)
        );
        """,
        """
            CREATE TABLE IF NOT EXISTS dim_parts (
            part_id VARCHAR(50) PRIMARY KEY,
            name VARCHAR(255),
            category VARCHAR(100),
            standard_cost DECIMAL(10,2),
            unit_of_measure VARCHAR(50),
            reorder_point INTEGER DEFAULT 0,
            safety_stock INTEGER DEFAULT 0
        );
        """,
        """
            CREATE TABLE IF NOT EXISTS dim_facilities (
            facility_id VARCHAR(100) PRIMARY KEY,
            facility_name VARCHAR(255),
            city VARCHAR(100),
            state VARCHAR(100),
            country VARCHAR(100),
            facility_type VARCHAR(100),
            region VARCHAR(100),
            location_code VARCHAR(100)
        );
        """,
        """
            CREATE TABLE IF NOT EXISTS dim_routes (
            route_id VARCHAR(100) PRIMARY KEY,
            origin_facility_id VARCHAR(100) REFERENCES dim_facilities(facility_id),
            origin_location_code VARCHAR(100),
            destination_country VARCHAR(100),
            typical_distance_miles INTEGER,
            typical_transit_days INTEGER,
            base_rate_per_mile DECIMAL(10,2),
            direction VARCHAR(50),
            destination_facility_id VARCHAR(100) REFERENCES dim_facilities(facility_id),
            destination_location_code VARCHAR(100)
        );
        """,
        """
            CREATE TABLE IF NOT EXISTS dim_products (
            product_id VARCHAR(100) PRIMARY KEY,
            name VARCHAR(255),
            type VARCHAR(100),
            key_features TEXT
        );
        """,
        # FACT
        """
            CREATE TABLE IF NOT EXISTS fact_events (
            event_id BIGSERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ NOT NULL,
            event_type VARCHAR(100) NOT NULL,
            payload JSONB
        );
        """,
        """
            CREATE TABLE IF NOT EXISTS fact_inventory_snapshots (
            snapshot_id BIGSERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ NOT NULL,
            part_id VARCHAR(50) REFERENCES dim_parts(part_id),
            qty_on_hand INTEGER
        );
        """,
        """
            CREATE TABLE IF NOT EXISTS fact_orders (
            order_id VARCHAR(100) PRIMARY KEY,
            customer_id VARCHAR(100) REFERENCES dim_customers(customer_id),
            order_date TIMESTAMPTZ,
            total_amount DECIMAL(12,2),
            status VARCHAR(50)
        );
        """,
        # STAGING
        """
            CREATE TABLE IF NOT EXISTS stg_orders (
            order_id UUID PRIMARY KEY,
            customer_id VARCHAR(100) REFERENCES dim_customers(customer_id),
            product_id VARCHAR(100) REFERENCES dim_products(product_id),
            order_date TIMESTAMPTZ,
            qty INTEGER,
            unit_price DECIMAL(10,2),
            line_total DECIMAL(12,2),
            promo_id UUID,
            source_event_id BIGINT REFERENCES fact_events(event_id)
        );
        """,
        """
            CREATE TABLE IF NOT EXISTS stg_loads (
            load_id UUID PRIMARY KEY,
            order_id UUID REFERENCES stg_orders(order_id),
            customer_id VARCHAR(100) REFERENCES dim_customers(customer_id),
            route_id VARCHAR(100) REFERENCES dim_routes(route_id),
            product_id VARCHAR(100) REFERENCES dim_products(product_id),
            qty INTEGER,
            weight_lbs DECIMAL(10,2),
            pieces INTEGER,
            load_status VARCHAR(50),
            scheduled_pickup TIMESTAMPTZ,
            scheduled_delivery TIMESTAMPTZ,
            created_at TIMESTAMPTZ,
            distance_miles INTEGER,
            source_event_id BIGINT REFERENCES fact_events(event_id)
        );    
        """,
        """
            CREATE TABLE IF NOT EXISTS stg_backorders (
            order_id UUID PRIMARY KEY REFERENCES stg_orders(order_id),
            customer_id VARCHAR(100) REFERENCES dim_customers(customer_id),
            product_id VARCHAR(100) REFERENCES dim_products(product_id),
            backorder_timestamp TIMESTAMPTZ,
            qty_backordered INTEGER,
            original_order_qty INTEGER,
            reason VARCHAR(100),
            source_event_id BIGINT REFERENCES fact_events(event_id)
        );
        """,
        """
            CREATE TABLE IF NOT EXISTS stg_delivery_events (
            event_id UUID PRIMARY KEY,
            load_id UUID REFERENCES stg_loads(load_id),
            event_type CHAR(1) NOT NULL,
            facility_id VARCHAR(100) REFERENCES dim_facilities(facility_id),
            event_timestamp TIMESTAMPTZ,
            scheduled_datetime TIMESTAMPTZ,
            actual_datetime TIMESTAMPTZ,
            detention_minutes INTEGER,
            on_time_flag BOOLEAN,
            source_event_id BIGINT REFERENCES fact_events(event_id)
        );    
        """,
        """
            CREATE TABLE IF NOT EXISTS stg_invoices (
            invoice_id UUID PRIMARY KEY,
            order_id UUID REFERENCES stg_orders(order_id),
            customer_id VARCHAR(100) REFERENCES dim_customers(customer_id),
            product_id VARCHAR(100) REFERENCES dim_products(product_id),
            qty INTEGER,
            amount DECIMAL(12,2),
            currency VARCHAR(10),
            due_date TIMESTAMPTZ,
            invoice_timestamp TIMESTAMPTZ,
            source_event_id BIGINT REFERENCES fact_events(event_id)
        );    
        """,
        """
            CREATE TABLE IF NOT EXISTS stg_demand_forecasts (
            snapshot_date DATE NOT NULL,
            product_id VARCHAR(100) REFERENCES dim_products(product_id),
            forecast_qty DECIMAL(12,2),
            horizon_days INTEGER,
            forecast_date DATE NOT NULL,
            source_event_id BIGINT REFERENCES fact_events(event_id),
            event_timestamp TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (source_event_id)
        );    
        """,
        """
            CREATE TABLE IF NOT EXISTS stg_production_jobs (
            job_id UUID PRIMARY KEY,
            product_id VARCHAR(100) REFERENCES dim_products(product_id),
            status VARCHAR(50),
            production_duration_hours INTEGER,
            source_event_id BIGINT REFERENCES fact_events(event_id),
            event_timestamp TIMESTAMPTZ NOT NULL
        );    
        """,
        """
            CREATE TABLE IF NOT EXISTS stg_purchase_orders (
            purchase_order_id UUID PRIMARY KEY,
            part_id VARCHAR(50) REFERENCES dim_parts(part_id),
            qty INTEGER,
            supplier_id VARCHAR(100) REFERENCES dim_suppliers(supplier_id),
            supplier_country VARCHAR(100),
            lead_time_hours INTEGER,
            eta TIMESTAMPTZ,
            is_reorder BOOLEAN,
            unit_cost DECIMAL(12,2),
            total_cost DECIMAL(14,2),
            base_cost DECIMAL(12,2),
            source_event_id BIGINT REFERENCES fact_events(event_id),
            event_timestamp TIMESTAMPTZ NOT NULL
        );    
        """,
        """
            CREATE TABLE IF NOT EXISTS stg_po_receipts (
            purchase_order_id UUID NOT NULL REFERENCES stg_purchase_orders(purchase_order_id),
            part_id VARCHAR(50) NOT NULL REFERENCES dim_parts(part_id),
            qty_ordered INTEGER NOT NULL,
            qty_received INTEGER NOT NULL,
            qty_rejected INTEGER NOT NULL DEFAULT 0,
            supplier_id VARCHAR(100) NOT NULL,
            was_partial_shipment BOOLEAN NOT NULL DEFAULT false,
            new_qty_on_hand INTEGER NOT NULL,
            received_timestamp TIMESTAMPTZ NOT NULL,
            source_event_id BIGINT NOT NULL REFERENCES fact_events(event_id),
            PRIMARY KEY (source_event_id)
        );
        """,
        # STATE
        """
            CREATE TABLE IF NOT EXISTS system_state (
            id INTEGER PRIMARY KEY DEFAULT 1,
            current_simulation_time TIMESTAMPTZ,
            tick_count BIGINT DEFAULT 0,
            status VARCHAR(20) DEFAULT 'stopped',
            CONSTRAINT single_row_const CHECK (id = 1)
        );
        """
    ]

    try:
        with engine.connect() as conn:
            trans = conn.begin()
            for sql in ddl_statements:
                conn.execute(text(sql))
            trans.commit()
            print("SUCCESS: Tables initialized successfully")
    except Exception as e:
        print(f"Error initializing tables: {e}")

if __name__ == "__main__":
    init_schema()
    init_tables()
