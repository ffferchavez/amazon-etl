# This module fetches real inventory data from Amazon SP API.
# Currently set up for production use with placeholder SKUs.

import os
from dotenv import load_dotenv
from sp_api.api import Inventory
from sp_api.base import Marketplaces, SellingApiException

load_dotenv()

def fetch_inventory(seller_skus=None):
    try:
        print("Fetching inventory from Amazon...")

        skus = seller_skus or ["SKU-001", "SKU-002"]

        inventory = []
        inventory_api = Inventory(marketplace=Marketplaces.US)

        for sku in skus:
            result = inventory_api.get_inventory_summary(sku=sku)
            inventory.append(result.payload)

        print(f"Fetched {len(inventory)} inventory records.")
        return inventory

    except SellingApiException as e:
        print("Amazon SP API error while fetching inventory:", e)
        return []
    
# RESPONSE STRUCTURE:
#     {
#   "asin": "B08XYZ1234",
#   "sku": "SKU-001",
#   "totalQuantity": 25,
#   "inventoryDetails": {
#     "fulfillmentCenterId": "AMAZON_US1",
#     "condition": "NewItem"
#   }
# }