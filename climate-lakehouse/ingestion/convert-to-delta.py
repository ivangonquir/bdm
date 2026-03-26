"""
convert-to-delta.py
────────────────────
Reads all NOAA CSV files using DuckDB and writes them as a
Delta Lake table to MinIO at s3://delta/noaa_bcn
"""

import duckdb
import sys

sys.path.insert(0, "/opt/airflow/ingestion")
from delta_utils import write_delta, ensure_bucket

# Ensure the delta bucket exists in MinIO
ensure_bucket("delta")

CSV_PATH = "/opt/airflow/landing-zone/structured/noaa/*.csv"

print("Starting Delta Lake conversion for NOAA data...")

try:
    conn = duckdb.connect()

    df = conn.execute(f"""
        SELECT
            CAST(date   AS TIMESTAMP) AS date,
            datatype,
            station,
            CAST(value  AS DOUBLE)    AS value,
            attributes
        FROM read_csv_auto('{CSV_PATH}', header=true)
    """).df()

    print(f"Read {len(df):,} records from CSV files")

    write_delta("noaa_bcn", df, mode="overwrite")

    print("Delta conversion complete!")

except Exception as e:
    print(f"Error during Delta conversion: {e}")
    sys.exit(1)

finally:
    conn.close()