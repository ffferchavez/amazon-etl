# This script currently runs with mock data but includes commented-out sections for production use with real Amazon SP API.

# from etl.extract_orders import fetch_orders
# from etl.transform_orders import transform_orders
# from etl.load_orders import insert_orders_to_db

# from etl.extract_products import fetch_products
# from etl.transform_products import transform_products
# from etl.load_products import insert_products_to_db

# from etl.extract_inventory import fetch_inventory
# from etl.transform_inventory import transform_inventory
# from etl.load_inventory import insert_inventory_to_db

from etl.mock_data import mock_orders, mock_products, mock_inventory
from etl.transform_orders import transform_orders
from etl.transform_products import transform_products
from etl.transform_inventory import transform_inventory
from etl.load_orders import insert_orders_to_db
from etl.load_products import insert_products_to_db
from etl.load_inventory import insert_inventory_to_db

def run_pipeline():
    # # Orders
    # raw_orders = fetch_orders()
    # transformed_orders = transform_orders(raw_orders)
    # insert_orders_to_db(transformed_orders)

    # # Products
    # raw_products = fetch_products()
    # transformed_products = transform_products(raw_products)
    # insert_products_to_db(transformed_products)

    # # Inventory
    # raw_inventory = fetch_inventory()
    # transformed_inventory = transform_inventory(raw_inventory)
    # insert_inventory_to_db(transformed_inventory)

    print("Running ETL with mock Amazon data...")

    # Orders
    transformed_orders = transform_orders(mock_orders)
    insert_orders_to_db(transformed_orders)

    # Products
    transformed_products = transform_products(mock_products)
    insert_products_to_db(transformed_products)

    # Inventory
    transformed_inventory = transform_inventory(mock_inventory)
    insert_inventory_to_db(transformed_inventory)

if __name__ == "__main__":
    run_pipeline()