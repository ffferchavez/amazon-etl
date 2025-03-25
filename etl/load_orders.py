import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

# This loads Amazon order data into MySQL, currently using mock data for testing.
# real SP API order data:
# {
#     'amazon_order_id': '123-1234567-1234567',
#     'order_status': 'Shipped',
#     'purchase_date': '2024-03-01 12:00:00',
#     'buyer_email': 'example@marketplace.amazon.com',
#     'order_total': 59.99,
#     'currency_code': 'USD'
# }

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
        # real SP API order:
        # {
        #     'amazon_order_id': '123-1234567-1234567',
        #     'order_status': 'Shipped',
        #     'purchase_date': '2024-03-01 12:00:00',
        #     'buyer_email': 'example@marketplace.amazon.com',
        #     'order_total': 59.99,
        #     'currency_code': 'USD'
        # }
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