import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

def insert_inventory_to_db(inventory):
    conn = pymysql.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DB"),
        port=int(os.getenv("MYSQL_PORT"))
    )
    cursor = conn.cursor()

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
    print(f"Inserted {len(inventory)} inventory records into MySQL.")