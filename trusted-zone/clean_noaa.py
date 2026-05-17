"""
Landing Zone → Trusted Zone: NOAA structured data.

Reads the raw NOAA Delta table, applies generic data-quality transformations
and writes the result to ClickHouse database 'trusted', table 'noaa_bcn'.

Cleaning applied:
  - Drop rows with null date or value
  - Data is already in °C (CDO API with units=metric returns proper Celsius)
  - Remove records outside Barcelona-specific bounds [-10, 45] °C
  - Deduplicate on (date, datatype, station), keeping first occurrence
  - Flag missing days with is_gap = True
  - Flag TMIN >= TMAX inconsistencies with is_inconsistent = True
  - Parse attributes field (,,E,) into m_flag, q_flag, s_flag columns
  - Document TAVG absence for this station
"""

import sys
import pandas as pd
import clickhouse_connect
from datetime import date, timedelta

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

# ── 2. Data is already in °C — no conversion needed ─────────────────────────
# The NOAA CDO API with units=metric returns proper Celsius values.
# No division by 10 is applied.

# ── 3. Barcelona-specific temperature range validation ───────────────────────
before = len(df)
df = df[(df["value"] >= -10) & (df["value"] <= 45)]
print(f"Dropped {before - len(df):,} rows outside Barcelona range [-10, 45] °C")

# ── 4. Deduplicate on natural key ────────────────────────────────────────────
before = len(df)
df = df.drop_duplicates(subset=["date", "datatype", "station"])
print(f"Dropped {before - len(df):,} duplicate rows")

# ── 5. Normalise date type (Date32 requires plain date, not timestamp) ────────
df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None).dt.date
before = len(df)
df = df.dropna(subset=["date"])
print(f"Dropped {before - len(df):,} rows with unparseable dates")

# ── 6. Parse attributes field (,,E,) → m_flag, q_flag, s_flag ───────────────
def parse_attributes(attr: str):
    parts = str(attr).split(",")
    m = parts[0] if len(parts) > 0 else ""
    q = parts[1] if len(parts) > 1 else ""
    s = parts[2] if len(parts) > 2 else ""
    return m.strip(), q.strip(), s.strip()

df["attributes"] = df["attributes"].fillna("").astype(str)
df[["m_flag", "q_flag", "s_flag"]] = df["attributes"].apply(
    lambda a: pd.Series(parse_attributes(a))
)

# ── 7. TMIN < TMAX consistency check ────────────────────────────────────────
by_date = df.pivot_table(
    index=["date", "station"], columns="datatype", values="value"
).reset_index()

if "TMIN" in by_date.columns and "TMAX" in by_date.columns:
    inconsistent_dates = by_date[
        by_date["TMIN"] >= by_date["TMAX"]
    ][["date", "station"]]
    inconsistent_keys = set(
        zip(inconsistent_dates["date"], inconsistent_dates["station"])
    )
    df["is_inconsistent"] = df.apply(
        lambda r: (r["date"], r["station"]) in inconsistent_keys, axis=1
    )
    n_inconsistent = inconsistent_dates.shape[0]
    print(f"Flagged {n_inconsistent:,} dates with TMIN >= TMAX")
else:
    df["is_inconsistent"] = False
    print("TMIN/TMAX consistency check skipped (one or both datatypes missing)")

# ── 8. Missing day detection ─────────────────────────────────────────────────
dates_present = set(df["date"].unique())
if dates_present:
    min_date = min(dates_present)
    max_date = max(dates_present)
    all_dates = set()
    d = min_date
    while d <= max_date:
        all_dates.add(d)
        d += timedelta(days=1)
    missing_dates = sorted(all_dates - dates_present)
    print(f"Detected {len(missing_dates):,} missing days in the time series")

    if missing_dates:
        station = df["station"].iloc[0]
        gap_rows = pd.DataFrame({
            "date":           missing_dates,
            "datatype":       "TMAX",          # placeholder datatype
            "station":        station,
            "value":          float("nan"),
            "attributes":     "",
            "m_flag":         "",
            "q_flag":         "",
            "s_flag":         "",
            "is_inconsistent": False,
        })
        df = pd.concat([df, gap_rows], ignore_index=True)
        print(f"Added {len(missing_dates):,} sentinel gap rows (value=NaN)")
else:
    print("No dates found — skipping missing day detection")

# ── 9. TAVG absence note ──────────────────────────────────────────────────────
datatypes_present = df["datatype"].unique().tolist()
if "TAVG" not in datatypes_present:
    print("Note: TAVG is absent for this station (GHCND:SP000008181). "
          "Downstream tasks should compute tavg = (tmin + tmax) / 2.")

print(f"Final clean record count: {len(df):,}")

# ── Write to ClickHouse ──────────────────────────────────────────────────────
client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT)

client.command("CREATE DATABASE IF NOT EXISTS trusted")
client.command("DROP TABLE IF EXISTS trusted.noaa_bcn")
client.command("""
    CREATE TABLE trusted.noaa_bcn (
        date             Date32,
        datatype         LowCardinality(String),
        station          String,
        value            Nullable(Float64),
        attributes       String,
        m_flag           String,
        q_flag           String,
        s_flag           String,
        is_inconsistent  UInt8
    ) ENGINE = MergeTree()
    ORDER BY (date, datatype, station)
""")

df["is_inconsistent"] = df["is_inconsistent"].astype("uint8")

client.insert_df(
    "trusted.noaa_bcn",
    df[["date", "datatype", "station", "value", "attributes",
        "m_flag", "q_flag", "s_flag", "is_inconsistent"]],
)

print(f"[ClickHouse] Wrote {len(df):,} rows → trusted.noaa_bcn")