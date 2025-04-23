import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def insert_inventory_to_db(inventory):
    schema = "amazon_data"
    table = "inventory_snapshot_2025_04"

    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        dbname=os.getenv("PG_DB"),
        port=int(os.getenv("PG_PORT")),
        sslmode="require"
    )
    cursor = conn.cursor()

    # Ensure schema exists
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    # Create the versioned table
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
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
        cursor.execute(f"""
            INSERT INTO {schema}.{table} (
                asin, sku, fulfillment_center, condition_type, quantity, last_updated
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            item.get('asin'),
            item.get('sku'),
            item.get('fulfillment_center'),
            item.get('condition_type'),
            item.get('quantity'),
            item.get('last_updated')
        ))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Inserted {len(inventory)} inventory records into {schema}.{table}.")

if __name__ == "__main__":
    from etl.transform_inventory import transform_inventory

    # Assume JSON was already fetched and saved to disk
    import json
    with open("amazon-fetched/inventory.json", "r") as f:
        raw = json.load(f)
    inventory = transform_inventory(raw["payload"]["inventorySummaries"])
    insert_inventory_to_db(inventory)