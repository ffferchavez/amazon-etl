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

# --- Database and UOM map setup ---
from sqlalchemy import create_engine, text
import pandas as pd

# Build the database URL from environment variables if DB_URL is not provided
db_url = os.getenv("DB_URL")
if not db_url:
    db_url = (
        f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@"
        f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/"
        f"{os.getenv('POSTGRES_DB')}?sslmode=require"
    )
engine = create_engine(db_url)

# --- Ensure staging table exists ---
with engine.begin() as conn:
    conn.execute(text("""
        CREATE SCHEMA IF NOT EXISTS amazon_data;
        CREATE TABLE IF NOT EXISTS amazon_data.inventory_snapshot (
            seller_sku                TEXT          NOT NULL,
            country                   TEXT          NOT NULL,
            snapshot_timestamp        TIMESTAMPTZ   NOT NULL,
            fulfillable_quantity      INTEGER       NOT NULL,
            unfulfillable_quantity    INTEGER       NOT NULL,
            inbound_working_quantity  INTEGER       NOT NULL,
            inbound_shipped_quantity  INTEGER       NOT NULL,
            inbound_received_quantity INTEGER       NOT NULL,
            reserved_quantity         INTEGER       NOT NULL,
            total_quantity            INTEGER       NOT NULL,
            PRIMARY KEY (seller_sku, country, snapshot_timestamp)
        );
    """))

 # --- Load UOM factors from Excel into product_uom table ---
excel_path = os.getenv('UOM_EXCEL_PATH', 'assets/Produktliste.xlsx')
# Inspect available sheets
xls = pd.ExcelFile(excel_path)
print("Available sheets in UOM file:", xls.sheet_names)
# Choose the correct sheet (replace 'YourSheetName' with the actual name)
sheet_name = os.getenv('UOM_SHEET_NAME', xls.sheet_names[0])
df_uom = pd.read_excel(excel_path, sheet_name=sheet_name)
# Clean up headers and drop blank rows
df_uom.columns = df_uom.columns.str.strip()
# ensure asin column exists even if missing from CSV
if "asin" not in df_uom.columns:
    df_uom["asin"] = None
df_uom = df_uom.rename(columns={'ASIN': 'master_sku', 'SKU': 'seller_sku', 'Einheiten': 'uom_factor'})
df_uom = df_uom[['master_sku', 'seller_sku', 'uom_factor']].dropna(subset=['master_sku', 'seller_sku'])
df_uom['uom_factor'] = pd.to_numeric(df_uom['uom_factor'], errors='coerce').fillna(1)
records = df_uom.to_dict(orient='records')
with engine.begin() as conn:
    conn.execute(text("""
        INSERT INTO amazon_data.product_uom (master_sku, seller_sku, uom_factor)
        VALUES (:master_sku, :seller_sku, :uom_factor)
        ON CONFLICT (master_sku, seller_sku) DO UPDATE SET
          uom_factor = EXCLUDED.uom_factor
    """), records)

# Load UOM factors into memory: keys are (asin, seller_sku)
with engine.connect() as conn:
    result = conn.execute(text(
        "SELECT master_sku, seller_sku, uom_factor FROM amazon_data.product_uom"
    )).mappings().all()
    UOM_MAP = {
        (row['master_sku'], row['seller_sku']): row['uom_factor']
        for row in result
    }


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
            max_results = int(os.getenv("SPAPI_MAX_RESULTS", 100))
            response = inventory_api.get_inventory_summary_marketplace(
                details=True,
                marketplaceIds=[marketplace.marketplace_id],
                nextToken=next_token,
                maxResultsPerPage=max_results
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
    # Normalize quantities by UOM and upsert into central DB
    normalize_and_upsert(all_inventory)
    return all_inventory


# --- Normalize and upsert inventory snapshot ---
def normalize_and_upsert(inventory_items):
    # take one timestamp for this batch
    batch_ts = datetime.utcnow()
    rows = []
    for item in inventory_items:
        master = item.get('asin')
        seller = item.get('sellerSku')
        factor = UOM_MAP.get((master, seller), 1)
        inv = item.get('inventoryDetails', {})
        fulfillable = inv.get('fulfillableQuantity', 0) * factor
        unfulfillable = inv.get('unfulfillableQuantity', {}).get('totalUnfulfillableQuantity', 0) * factor
        inbound_working = inv.get('inboundWorkingQuantity', 0) * factor
        inbound_shipped = inv.get('inboundShippedQuantity', 0) * factor
        inbound_received = inv.get('inboundReceivingQuantity', 0) * factor
        reserved = inv.get('reservedQuantity', {}).get('totalReservedQuantity', 0) * factor
        total = item.get('totalQuantity', 0) * factor
        # choose per-item timestamp if available, else batch_ts
        raw_ts = item.get('lastUpdatedTime')
        if raw_ts:
            try:
                snapshot = datetime.fromisoformat(raw_ts.replace('Z', '+00:00'))
            except Exception:
                print(f"⚠️ Could not parse timestamp {raw_ts} for SKU {item.get('sellerSku')}, defaulting to batch time")
                snapshot = batch_ts
        else:
            snapshot = batch_ts
        rows.append({
            "seller_sku": seller,
            "country": item.get('country'),
            "snapshot_timestamp": snapshot,
            "fulfillable_quantity": fulfillable,
            "unfulfillable_quantity": unfulfillable,
            "inbound_working_quantity": inbound_working,
            "inbound_shipped_quantity": inbound_shipped,
            "inbound_received_quantity": inbound_received,
            "reserved_quantity": reserved,
            "total_quantity": total
        })
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO amazon_data.inventory_snapshot
                (seller_sku, country, snapshot_timestamp, fulfillable_quantity,
                 unfulfillable_quantity, inbound_working_quantity,
                 inbound_shipped_quantity, inbound_received_quantity,
                 reserved_quantity, total_quantity)
            VALUES
                (:seller_sku, :country, :snapshot_timestamp, :fulfillable_quantity,
                 :unfulfillable_quantity, :inbound_working_quantity,
                 :inbound_shipped_quantity, :inbound_received_quantity,
                 :reserved_quantity, :total_quantity)
            ON CONFLICT (seller_sku, country, snapshot_timestamp) DO UPDATE SET
                fulfillable_quantity = EXCLUDED.fulfillable_quantity,
                unfulfillable_quantity = EXCLUDED.unfulfillable_quantity,
                inbound_working_quantity = EXCLUDED.inbound_working_quantity,
                inbound_shipped_quantity = EXCLUDED.inbound_shipped_quantity,
                inbound_received_quantity = EXCLUDED.inbound_received_quantity,
                reserved_quantity = EXCLUDED.reserved_quantity,
                total_quantity = EXCLUDED.total_quantity
        """), rows)

if __name__ == "__main__":
    run_daily_inventory_report()