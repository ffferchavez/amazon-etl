import os
from dotenv import load_dotenv
from sp_api.api import Orders
from sp_api.base import Marketplaces, SellingApiException

# This file contains the function to fetch real order data from Amazon SP API.
# It is designed to be replaced or mocked during development.

load_dotenv()

def fetch_orders():
    try:
        print("📦 Fetching orders from Amazon...")

        marketplace_code = os.getenv("AMAZON_MARKETPLACE", "US").upper()
        created_after = os.getenv("CREATED_AFTER", "2024-01-01T00:00:00Z")
        marketplace = getattr(Marketplaces, marketplace_code)

        orders_api = Orders(marketplace=marketplace)
        response = orders_api.get_orders(CreatedAfter=created_after)
        orders = response.payload.get("Orders", [])

        while "NextToken" in response.payload:
            next_token = response.payload["NextToken"]
            print("➡️ Fetching next page of orders...")
            response = orders_api.get_orders(NextToken=next_token)
            orders.extend(response.payload.get("Orders", []))

        print(f"✅ Fetched {len(orders)} orders.")
        return orders

    except SellingApiException as e:
        print("❌ Amazon SP API error:", e)
        return []