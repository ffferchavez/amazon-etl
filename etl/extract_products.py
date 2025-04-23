# fetches real product data from Amazon SP API.
# It is currently a placeholder with the structure ready for production use.

import os
from dotenv import load_dotenv
from sp_api.api import ListingsItems
from sp_api.base import Marketplaces, SellingApiException

load_dotenv()

def fetch_products(seller_skus=None):
    try:
        print("🛍️ Fetching products from Amazon...")

        marketplace_code = os.getenv("AMAZON_MARKETPLACE", "US").upper()
        marketplace = getattr(Marketplaces, marketplace_code)
        seller_id = os.getenv("SP_API_SELLER_ID")

        skus = seller_skus or ["SKU-001", "SKU-002"]

        products = []
        for sku in skus:
            result = ListingsItems(marketplace=marketplace).get_listings_item(
                sellerId=seller_id,
                sku=sku,
                includedData=["attributes", "summaries", "issues", "fulfillmentAvailability"]
            )
            products.append(result.payload)

        print(f"✅ Fetched {len(products)} products.")
        return products

    except SellingApiException as e:
        print("❌ Amazon SP API error while fetching products:", e)
        return []