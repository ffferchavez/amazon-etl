import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def insert_inventory_to_db(inventory):
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
        CREATE TABLE IF NOT EXISTS amazon_inventory (
            id SERIAL PRIMARY KEY,
            asin TEXT,
            sku TEXT,
            fulfillment_center TEXT,
            condition_type TEXT,
            quantity INTEGER,
            last_updated TIMESTAMP
        );
    """)

    for item in inventory:
        cursor.execute("""
            INSERT INTO amazon_inventory (
                asin, sku, fulfillment_center, condition_type, quantity, last_updated
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            item['asin'],
            item['sku'],
            item['fulfillment_center'],
            item['condition_type'],
            item['quantity'],
            item['last_updated']
        ))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Inserted {len(inventory)} inventory records into PostgreSQL.")