import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def insert_inventory_to_db(inventory):
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB"),
        port=int(os.getenv("POSTGRES_PORT"))
    )
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO amazon_inventory (
            asin, sku, fulfillment_center, condition_type, quantity, last_updated, country
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    successful_inserts = 0
    for item in inventory:
        try:
            cursor.execute(insert_query, (
                item.get('asin', None),
                item.get('sku', None),
                item.get('fulfillment_center', None),
                item.get('condition_type', None),
                item.get('quantity', 0),
                item.get('last_updated', None),
                item.get('country', None)
            ))
            successful_inserts += 1
        except Exception as e:
            print(f"❌ Failed to insert item {item.get('sku')}: {e}")

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Inserted {successful_inserts} inventory records into PostgreSQL.")