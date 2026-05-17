"""
Landing Zone → Trusted Zone: NOAA structured data.

Reads the raw NOAA Delta table, applies generic data-quality transformations
(unit conversion, range validation, deduplication) and writes the result to
ClickHouse database 'trusted', table 'noaa_bcn'.

Cleaning applied:
  - Drop rows with null date or value
  - Convert value from NOAA tenths-of-°C to °C (CDO API standard)
  - Remove records outside global temperature bounds [-90, 60] °C
  - Deduplicate on (date, datatype, station), keeping first occurrence
  - Strip timezone info from timestamps (ClickHouse DateTime is tz-naive)
"""

import sys
import pandas as pd
import clickhouse_connect

sys.path.insert(0, "/opt/airflow/ingestion")
from delta_utils import read_delta

CH_HOST = "clickhouse"
CH_PORT = 8123

print("=== NOAA Trusted Zone Cleaning ===")

# ── Read from Delta ──────────────────────────────────────────────────────────
table = read_delta("noaa_bcn")
df = table.to_pandas()
print(f"Read {len(df):,} records from Delta landing zone")

# ── 1. Drop null date / value ────────────────────────────────────────────────
before = len(df)
df = df.dropna(subset=["date", "value"])
print(f"Dropped {before - len(df):,} rows with null date or value")

# ── 2. Convert tenths-of-°C → °C (NOAA CDO API stores temps × 10) ───────────
df["value"] = df["value"] / 10.0

# ── 3. Validate temperature range ───────────────────────────────────────────
before = len(df)
df = df[(df["value"] >= -90) & (df["value"] <= 60)]
print(f"Dropped {before - len(df):,} rows outside valid range [-90, 60] °C")

# ── 4. Deduplicate on natural key ────────────────────────────────────────────
before = len(df)
df = df.drop_duplicates(subset=["date", "datatype", "station"])
print(f"Dropped {before - len(df):,} duplicate rows")

# ── 5. Normalise types ───────────────────────────────────────────────────────
df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
df["attributes"] = df["attributes"].fillna("").astype(str)

print(f"Final clean record count: {len(df):,}")

# ── Write to ClickHouse ──────────────────────────────────────────────────────
client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT)

client.command("CREATE DATABASE IF NOT EXISTS trusted")
client.command("DROP TABLE IF EXISTS trusted.noaa_bcn")
client.command("""
    CREATE TABLE trusted.noaa_bcn (
        date        DateTime64(3),
        datatype    LowCardinality(String),
        station     String,
        value       Float64,
        attributes  String
    ) ENGINE = MergeTree()
    ORDER BY (date, datatype, station)
""")

client.insert_df(
    "trusted.noaa_bcn",
    df[["date", "datatype", "station", "value", "attributes"]],
)

print(f"[ClickHouse] Wrote {len(df):,} rows → trusted.noaa_bcn")
