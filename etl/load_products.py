import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def insert_products_to_db(products):
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
        CREATE TABLE IF NOT EXISTS amazon_products (
            id SERIAL PRIMARY KEY,
            asin TEXT,
            sku TEXT,
            product_title TEXT,
            price NUMERIC,
            currency_code TEXT,
            stock_quantity INTEGER,
            last_updated TIMESTAMP
        );
    """)

    for product in products:
        cursor.execute("""
            INSERT INTO amazon_products (
                asin, sku, product_title, price, currency_code, stock_quantity, last_updated
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (sku) DO UPDATE SET
                price = EXCLUDED.price,
                currency_code = EXCLUDED.currency_code,
                stock_quantity = EXCLUDED.stock_quantity,
                last_updated = EXCLUDED.last_updated;
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
    print(f"✅ Inserted or updated {len(products)} products in PostgreSQL.")