from etl.extract import fetch_orders
from etl.transform import transform_orders
from etl.load import insert_orders_to_db

def run_pipeline():
    raw_orders = fetch_orders()
    if not raw_orders:
        print("No orders found.")
        return

    transformed_orders = transform_orders(raw_orders)
    insert_orders_to_db(transformed_orders)

if __name__ == "__main__":
    run_pipeline()