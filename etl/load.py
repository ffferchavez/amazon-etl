import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

def insert_orders_to_db(orders):
    conn = pymysql.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DB"),
        port=int(os.getenv("MYSQL_PORT"))
    )
    cursor = conn.cursor()

    for order in orders:
        cursor.execute("""
            INSERT IGNORE INTO amazon_orders (
                amazon_order_id, order_status, purchase_date, buyer_email, order_total, currency_code
            ) VALUES (%s, %s, %s, %s, %s, %s)
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
    print(f"Inserted {len(orders)} orders into MySQL.")