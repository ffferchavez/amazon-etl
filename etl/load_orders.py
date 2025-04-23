import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def insert_orders_to_db(orders):
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        dbname=os.getenv("PG_DB"),
        port=int(os.getenv("PG_PORT")),
        sslmode="require"
    )
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS amazon_orders (
            id SERIAL PRIMARY KEY,
            amazon_order_id TEXT UNIQUE,
            order_status TEXT,
            purchase_date TIMESTAMP,
            buyer_email TEXT,
            order_total NUMERIC,
            currency_code TEXT
        );
    """)

    for order in orders:
        cursor.execute("""
            INSERT INTO amazon_orders (
                amazon_order_id, order_status, purchase_date, buyer_email, order_total, currency_code
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (amazon_order_id) DO NOTHING;
        """, (
            order['amazon_order_id'],
            order['order_status'],
            order['purchase_date'],
            order['buyer_email'],
            order['order_total'],
            order['currency_code']
        ))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Inserted {len(orders)} orders into PostgreSQL.")