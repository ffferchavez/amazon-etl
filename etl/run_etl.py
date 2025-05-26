from etl.extract_inventory import run_daily_inventory_report
from etl.transform_inventory import transform_inventory
from etl.load_inventory import insert_inventory_to_db

def run_pipeline():
    raw_inventory = run_daily_inventory_report()
    if not raw_inventory:
        print("⚠️ No data to process.")
        return

    transformed = transform_inventory(raw_inventory)
    insert_inventory_to_db(transformed)

if __name__ == "__main__":
    run_pipeline()