import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

# This inserts product data into MySQL, currently using mock data.
# real SP API product structure:
# {
#     'asin': 'B08XYZ1234',
#     'sku': 'SKU-001',
#     'product_title': 'Wireless Mouse',
#     'price': 19.99,
#     'currency_code': 'USD',
#     'stock_quantity': 85,
#     'last_updated': '2024-03-24 12:00:00'
# }

def insert_products_to_db(products):
    conn = pymysql.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DB"),
        port=int(os.getenv("MYSQL_PORT"))
    )
    cursor = conn.cursor()

    for product in products:
        # real SP API product:
        # {
        #     'asin': 'B08XYZ1234',
        #     'sku': 'SKU-001',
        #     'product_title': 'Wireless Mouse',
        #     'price': 19.99,
        #     'currency_code': 'USD',
        #     'stock_quantity': 85,
        #     'last_updated': '2024-03-24 12:00:00'
        # }
        cursor.execute("""
            INSERT INTO amazon_products (
                asin, sku, product_title, price, currency_code, stock_quantity, last_updated
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                price = VALUES(price),
                currency_code = VALUES(currency_code),
                stock_quantity = VALUES(stock_quantity),
                last_updated = VALUES(last_updated)
        """, (
            product['asin'],
            product['sku'],
            product['product_title'],
            product['price'],
            product['currency_code'],
            product['stock_quantity'],
            product['last_updated']
        ))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Inserted or updated {len(products)} products in MySQL.")