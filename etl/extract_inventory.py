"""
This script fetches full Amazon FBA inventory data from SP-API for multiple EU marketplaces.
It collects detailed inventory metrics per SKU and prints a daily summary.
"""

import os
from dotenv import load_dotenv
load_dotenv()
import boto3
from sp_api.api import Inventories
from sp_api.api import ListingsItems
from sp_api.base import SellingApiException, Marketplaces
from datetime import datetime


EU_MARKETPLACES = {
    'DE': Marketplaces.DE,
    'FR': Marketplaces.FR,
    'IT': Marketplaces.IT,
    'ES': Marketplaces.ES,
    'NL': Marketplaces.NL,
    'PL': Marketplaces.PL,
    'SE': Marketplaces.SE,
    'BE': Marketplaces.BE,
}


# --- AWS STS temporary credentials for SP-API authentication ---
def get_spapi_credentials():
    client = boto3.client(
        'sts',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name='us-east-1'
    )
    response = client.assume_role(
        RoleArn=os.getenv("SPAPI_ROLE_ARN"),
        RoleSessionName="SPAPISession"
    )
    creds = response['Credentials']
    return {
        "refresh_token": os.getenv("SPAPI_REFRESH_TOKEN"),
        "lwa_app_id": os.getenv("SPAPI_CLIENT_ID"),
        "lwa_client_secret": os.getenv("SPAPI_CLIENT_SECRET"),
        "aws_access_key": creds["AccessKeyId"],
        "aws_secret_key": creds["SecretAccessKey"],
        "role_arn": os.getenv("SPAPI_ROLE_ARN"),
        "session_token": creds["SessionToken"],
    }

def fetch_inventory_for_marketplace(marketplace, country_code):
    try:
        print(f"\n🌍 Fetching inventory for {country_code}...")
        credentials = get_spapi_credentials()
        inventory_api = Inventories(marketplace=marketplace, credentials=credentials)
        all_summaries = []
        next_token = None

        while True:
            response = inventory_api.get_inventory_summary_marketplace(
                details=True,
                marketplaceIds=[marketplace.marketplace_id],
                nextToken=next_token
            )
            summaries = response.payload.get("inventorySummaries", [])
            for item in summaries:
                item['country'] = country_code
            all_summaries.extend(summaries)

            next_token = response.payload.get("nextToken")
            if not next_token:
                break
        print(f"✅ {country_code}: {len(all_summaries)} items")
        return all_summaries
    except Exception as e:
        print(f"❌ {country_code} failed: {e}")
        return []

def enrich_inventory_with_asins(marketplace, country_code, inventory):
    try:
        print(f"\n🔍 Enriching ASINs for {country_code}...")
        credentials = get_spapi_credentials()
        listings_api = ListingsItems(marketplace=marketplace, credentials=credentials)
        enriched = []

        for item in inventory:
            sku = item.get("sellerSku")
            if item.get("asin"):
                enriched.append(item)
                continue
            try:
                response = listings_api.get_listings_item(sellerSku=sku, marketplaceIds=[marketplace.marketplace_id])
                asin = response.payload.get("asin")
                item["asin"] = asin
            except Exception as inner:
                print(f"⚠️ Could not fetch ASIN for SKU {sku}: {inner}")
            enriched.append(item)

        return enriched
    except Exception as e:
        print(f"❌ ASIN enrichment failed for {country_code}: {e}")
        return inventory

def run_daily_inventory_report():
    print(f"\n📦 Daily Amazon FBA Inventory Report - {datetime.now().strftime('%Y-%m-%d')}")
    print("=" * 60)

    all_inventory = []
    for country, marketplace in EU_MARKETPLACES.items():
        inv = fetch_inventory_for_marketplace(marketplace, country)
        if not inv:
            continue
        enriched_inv = enrich_inventory_with_asins(marketplace, country, inv)
        all_inventory.extend(enriched_inv)

    if not all_inventory:
        print("⚠️ No inventory data returned.")
        return []

    print(f"\n✅ Total enriched inventory items: {len(all_inventory)}")
    print("=" * 60)
    return all_inventory

if __name__ == "__main__":
    run_daily_inventory_report()