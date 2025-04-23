# This module fetches real inventory data from Amazon SP API.
# Currently set up for production use with placeholder SKUs.

import os
from dotenv import load_dotenv
from sp_api.api.inventory import Inventory
from sp_api.base import Marketplaces, SellingApiException

load_dotenv()

def fetch_inventory():
    try:
        print("📦 Fetching inventory from Amazon...")

        inventory = []
        marketplace = os.getenv("AMAZON_MARKETPLACE", "DE").upper()
        inventory_api = Inventory(marketplace=getattr(Marketplaces, marketplace))

        response = inventory_api.get_inventory_summary()
        summaries = response.payload.get("inventorySummaries", [])
        inventory.extend(summaries)

        while "nextToken" in response.payload:
            print("🔁 Fetching next page...")
            response = inventory_api.get_inventory_summary(nextToken=response.payload["nextToken"])
            inventory.extend(response.payload.get("inventorySummaries", []))

        # Enrich and flatten
        for item in inventory:
            details = item.get("inventoryDetails", {})
            item["fulfillment_center"] = details.get("fulfillmentCenterId")
            item["condition_type"] = details.get("condition")
            item["quantity"] = item.get("totalQuantity", 0)

        print(f"✅ Fetched {len(inventory)} inventory records.")
        return inventory

    except SellingApiException as e:
        print("❌ Amazon SP API error while fetching inventory:", e)
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

if __name__ == "__main__":
    inventory_data = fetch_inventory()
    
    os.makedirs("amazon-fetched", exist_ok=True)
    with open("amazon-fetched/inventory.json", "w", encoding="utf-8") as f:
        import json
        json.dump({"payload": {"inventorySummaries": inventory_data}}, f, ensure_ascii=False, indent=2)

    print(f"📁 Saved inventory snapshot to amazon-fetched/inventory.json")