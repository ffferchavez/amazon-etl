import os
from dotenv import load_dotenv
from sp_api.api import Orders
from sp_api.base import Marketplaces, SellingApiException

load_dotenv()

def fetch_orders():
    try:
        print("Fetching orders from Amazon...")

        orders_api = Orders(marketplace=Marketplaces.US)
        response = orders_api.get_orders(CreatedAfter='2024-01-01T00:00:00Z')

        orders = response.payload.get("Orders", [])
        print(f"Fetched {len(orders)} orders.")
        return orders

    except SellingApiException as e:
        print("Amazon SP API error:", e)
        return []