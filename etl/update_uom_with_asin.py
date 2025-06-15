# scripts/update_uom_with_asin.py

import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

# ——————————————————————————————————————————————
# 1) connect to your database
# ——————————————————————————————————————————————
db_url = os.getenv("DB_URL")
if not db_url:
    db_url = (
        f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@"
        f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/"
        f"{os.getenv('POSTGRES_DB')}?sslmode=require"
    )
engine = create_engine(db_url)

# ——————————————————————————————————————————————
# 2) load your current UOM file
# ——————————————————————————————————————————————
uom_path = "assets/clean_uom.csv"
df_uom = pd.read_csv(uom_path, dtype=str)

# ——————————————————————————————————————————————
# 3) fetch the latest ASIN per seller_sku from your archive
# ——————————————————————————————————————————————
sql = """
SELECT seller_sku, asin
FROM (
  SELECT
    seller_sku,
    asin,
    ROW_NUMBER() OVER (PARTITION BY seller_sku ORDER BY snapshot_date DESC) AS rn
  FROM amazon_data.inventory_snapshot_archive
) t
WHERE rn = 1
"""
df_map = pd.read_sql(sql, engine)

# ——————————————————————————————————————————————
# 4) merge them
# ——————————————————————————————————————————————
df_merged = (
    df_uom
    .merge(df_map, on="seller_sku", how="left")
    .rename(columns={"asin_y": "asin"})  # if your clean_uom.csv already had an asin column it’ll end up _x/_y
)

# ——————————————————————————————————————————————
# 5) write out the new CSV
# ——————————————————————————————————————————————
out_path = "assets/clean_uom_with_asin.csv"
df_merged.to_csv(out_path, index=False)
print(f"Wrote {out_path} — now includes an `asin` column for every SKU.")