"""
Landing Zone → Trusted Zone: OpenWeatherMap semi-structured data.

Reads the raw weather_stream Delta table, applies generic data-quality
transformations and writes cleaned documents to MongoDB database 'trusted',
collection 'weather_stream'.

Cleaning applied:
  - Deduplicate on event_ts (same event must not appear twice)
  - Validate temperature range [-50, 60] °C
  - Validate humidity range [0, 100] %
  - Fill missing string fields with empty string defaults
"""

import sys
import numpy as np
import pandas as pd
from pymongo import MongoClient

sys.path.insert(0, "/opt/airflow/ingestion")
from delta_utils import read_delta

MONGO_URI = "mongodb://mongodb:27017/"

print("=== OpenWeather Trusted Zone Cleaning ===")

# ── Read from Delta ──────────────────────────────────────────────────────────
table = read_delta("weather_stream")
df = table.to_pandas()
print(f"Read {len(df):,} records from Delta landing zone")

if df.empty:
    print("No data to process — exiting.")
    sys.exit(0)

# ── 1. Deduplicate on event timestamp ────────────────────────────────────────
before = len(df)
df = df.drop_duplicates(subset=["event_ts"])
print(f"Dropped {before - len(df):,} duplicate events")

# ── 2. Validate temperature range ───────────────────────────────────────────
before = len(df)
mask = df["temp_c"].isna() | ((df["temp_c"] >= -50) & (df["temp_c"] <= 60))
df = df[mask]
print(f"Dropped {before - len(df):,} rows with invalid temperature")

# ── 3. Validate humidity range ───────────────────────────────────────────────
before = len(df)
mask = df["humidity_pct"].isna() | ((df["humidity_pct"] >= 0) & (df["humidity_pct"] <= 100))
df = df[mask]
print(f"Dropped {before - len(df):,} rows with invalid humidity")

# ── 4. Fill missing string fields ────────────────────────────────────────────
df["city"]         = df["city"].fillna("Barcelona")
df["weather_main"] = df["weather_main"].fillna("")
df["weather_desc"] = df["weather_desc"].fillna("")

print(f"Final clean record count: {len(df):,}")

# ── Convert to plain Python dicts (MongoDB-safe) ─────────────────────────────
records = df.to_dict("records")
for r in records:
    for key in ("ingested_at", "event_ts"):
        val = r.get(key)
        if hasattr(val, "to_pydatetime"):
            r[key] = val.to_pydatetime()
    # Convert any remaining numpy scalars
    r = {k: (v.item() if isinstance(v, np.generic) else v) for k, v in r.items()}

# ── Write to MongoDB ─────────────────────────────────────────────────────────
client = MongoClient(MONGO_URI)
collection = client["trusted"]["weather_stream"]
collection.drop()

if records:
    collection.insert_many(records)

print(f"[MongoDB] Wrote {len(records)} documents → trusted.weather_stream")
client.close()
