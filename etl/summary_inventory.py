import os
from datetime import date
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")

engine = create_engine(
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

def summarize_inventory(uom_file="assets/clean_uom_with_asin.csv"):
    # STEP 1: Load existing snapshot into pandas before archiving
    df_inventory = pd.read_sql(
        "SELECT * FROM amazon_data.inventory_snapshot",
        con=engine
    )
    # Ensure `asin` column exists even if inventory_snapshot lacks it
    if 'asin' not in df_inventory.columns:
        df_inventory['asin'] = None
    # Archive yesterday's snapshot, then reset staging
    with engine.begin() as conn:
        # Ensure archive table exists and has a snapshot_date column
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS amazon_data.inventory_snapshot_archive
            (LIKE amazon_data.inventory_snapshot INCLUDING ALL);
        """))
        conn.execute(text("""
            ALTER TABLE amazon_data.inventory_snapshot_archive
            ADD COLUMN IF NOT EXISTS snapshot_date date;
        """))
        conn.execute(text("""
            ALTER TABLE amazon_data.inventory_snapshot
            ADD COLUMN IF NOT EXISTS asin TEXT;
        """))
        # Copy current snapshot into archive with a date partition
        conn.execute(text("""
            INSERT INTO amazon_data.inventory_snapshot_archive
            SELECT *, snapshot_timestamp::date as snapshot_date
            FROM amazon_data.inventory_snapshot
            ON CONFLICT (seller_sku, country, snapshot_timestamp) DO NOTHING;
        """))
        # Reset staging tables for fresh load
        conn.execute(text("TRUNCATE TABLE amazon_data.inventory_snapshot CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS amazon_data.inventory_summary"))
    # --- START DATA CLEANING BLOCK ---
    # 1) Drop exact duplicates
    df_inventory = df_inventory.drop_duplicates(
        subset=["seller_sku","country","total_quantity","snapshot_timestamp"],
        keep="last"
    )
    # 2) Normalize SKUs
    df_inventory["seller_sku"] = (
        df_inventory["seller_sku"]
        .astype(str)
        .str.strip()
        .str.upper()
    )
    # --- END DATA CLEANING BLOCK ---

    # Keep latest snapshot per (seller_sku, country)
    df_inventory['snapshot_timestamp'] = pd.to_datetime(df_inventory['snapshot_timestamp'])
    df_latest = df_inventory.sort_values("snapshot_timestamp").drop_duplicates(subset=["seller_sku", "country"], keep="last")

    # Load cleaned UOM data from CSV
    df_uom = pd.read_csv(uom_file)
    df_uom.columns = [col.strip().lower() for col in df_uom.columns]
    df_uom = df_uom[["asin", "seller_sku", "uom", "product_name"]]
    # Normalize ASIN and SKU
    df_uom["seller_sku"] = (
        df_uom["seller_sku"]
        .astype(str)
        .str.strip()
        .str.upper()
        .str.strip('"')
    )
    df_uom["asin"] = df_uom["asin"].astype(str).str.strip().str.strip('"')

    # Join with inventory
    df_merged = pd.merge(df_latest, df_uom, on="seller_sku", how="left")
    # Ensure we have a single `asin` column after the merge
    if 'asin_y' in df_merged.columns:
        df_merged = df_merged.rename(columns={'asin_y': 'asin'})
    elif 'asin_x' in df_merged.columns:
        df_merged = df_merged.rename(columns={'asin_x': 'asin'})

    # Drop any leftover merge artifacts
    for col in list(df_merged.columns):
        if col.endswith('_x') or col.endswith('_y'):
            df_merged.drop(columns=[col], inplace=True)
    # Ensure `asin` column is present (may be missing if UOM CSV lacked it)
    if 'asin' not in df_merged.columns:
        df_merged['asin'] = None
    # --- START MISSING UOM DEFAULTS ---
    missing_uom = (
        df_merged[df_merged["asin"].isna()]
        .drop_duplicates(subset=["seller_sku"])[["seller_sku"]]
    )
    if not missing_uom.empty:
        defaults = missing_uom.assign(asin="", uom=1, product_name="(unknown)")
        df_uom = pd.concat([df_uom, defaults], ignore_index=True)
        df_merged = pd.merge(df_latest, df_uom, on="seller_sku", how="left")
        # Ensure we have a single `asin` column after the missing-UOM merge
        if 'asin_y' in df_merged.columns:
            df_merged.rename(columns={'asin_y': 'asin'}, inplace=True)
        elif 'asin_x' in df_merged.columns:
            df_merged.rename(columns={'asin_x': 'asin'}, inplace=True)
        # Drop leftover merge artifacts
        for col in list(df_merged.columns):
            if col.endswith('_x') or col.endswith('_y'):
                df_merged.drop(columns=[col], inplace=True)
    # --- END MISSING UOM DEFAULTS ---
    df_merged["uom"] = df_merged["uom"].fillna(1)
    df_merged["quantity_on_hand"] = df_merged["total_quantity"] * df_merged["uom"]

    summary = (
        df_merged.groupby(["country", "asin", "product_name"])["quantity_on_hand"]
        .sum()
        .reset_index()
        .sort_values(["country", "asin"])
    )

    print("✅ Inventory summary by country, ASIN, and product name:")
    print(summary)

    # Save summary to PostgreSQL table
    # Ensure schema exists
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS amazon_data;"))
    # Write summary table, replacing any existing
    summary.to_sql(
        "inventory_summary",
        con=engine,
        schema="amazon_data",
        index=False,
        if_exists="replace"
    )

    # Save summary to CSV
    today_str = date.today().isoformat()
    os.makedirs("exports", exist_ok=True)
    summary.to_csv(f"exports/inventory_summary_{today_str}.csv", index=False)

    print(f"\n✅ Saved summary to DB table 'amazon_data.inventory_summary' and CSV file for {today_str}")

if __name__ == "__main__":
    summarize_inventory()