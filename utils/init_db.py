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
            supplier_id UUID PRIMARY KEY,
            name VARCHAR(255),
            country VARCHAR(100),
            reliability_score DECIMAL(3,2),
            risk_factor VARCHAR(50),
            price_multiplier DECIMAL(4,2)
        );
        """,
        """
            CREATE TABLE IF NOT EXISTS dim_customers (
            customer_id UUID PRIMARY KEY,
            company_name VARCHAR(255),
            region VARCHAR(100),
            contract_priority VARCHAR(50),
            shipping_address VARCHAR(500),
            penalty_clauses JSONB
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
            customer_id UUID REFERENCES dim_customers(customer_id),
            order_date TIMESTAMPTZ,
            total_amount DECIMAL(12,2),
            status VARCHAR(50)
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
