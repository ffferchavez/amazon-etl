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
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS amazon_data.amazon_inventory (
            id SERIAL PRIMARY KEY,
            asin VARCHAR(20),
            sku VARCHAR(100),
            fulfillment_center VARCHAR(50),
            condition_type VARCHAR(50),
            quantity INTEGER,
            last_updated TIMESTAMP,
            country VARCHAR(10),
            UNIQUE (asin, sku, country)
        );
    """)

    insert_query = """
        INSERT INTO amazon_data.amazon_inventory (
            asin, sku, fulfillment_center, condition_type, quantity, last_updated, country
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (asin, sku, country) DO NOTHING
    """

    FULFILLMENT_COUNTRY_MAP = {
        'FRA': 'DE', 'DEH': 'DE', 'MAD': 'ES', 'MXP': 'IT', 'CDG': 'FR', 'AMS': 'NL',
        'WAW': 'PL', 'STO': 'SE', 'BRU': 'BE'
    }

    def guess_country_from_fc(fc_id, default):
        if fc_id and isinstance(fc_id, str):
            prefix = fc_id[:3].upper()
            return FULFILLMENT_COUNTRY_MAP.get(prefix, default)
        return default

    successful_inserts = 0
    for item in inventory:
        try:
            fc = item.get('fulfillment_center', None)
            country = item.get('country', None)
            country = guess_country_from_fc(fc, country)
            cursor.execute(insert_query, (
                item.get('asin', None),
                item.get('sku', None),
                fc,
                item.get('condition_type', None),
                item.get('quantity', 0),
                item.get('last_updated', None),
                country
            ))
            successful_inserts += 1
        except Exception as e:
            print(f"❌ Failed to insert item {item.get('sku')}: {e}")

    cursor.close()
    conn.close()

    print(f"✅ Inserted {successful_inserts} inventory records into PostgreSQL.")