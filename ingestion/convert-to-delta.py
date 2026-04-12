"""
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

conn = None

try:
    conn = duckdb.connect()

    files = conn.execute(f"""
        SELECT count(*) 
        FROM glob('{CSV_PATH}')
    """).fetchone()[0]

    if files == 0:
        raise Exception("No CSV files found in landing zone")

    print(f"Found {files} CSV files")

    df = conn.execute(f"""
        SELECT
            TRY_CAST(date AS TIMESTAMP) AS date,
            datatype,
            station,
            TRY_CAST(value AS DOUBLE) AS value,
            attributes
        FROM read_csv_auto(
            '{CSV_PATH}',
            header=true,
            ignore_errors=true
        )
    """).df()

    if df.empty:
        raise Exception("DataFrame is empty after reading CSVs")

    print(f"Read {len(df):,} records from CSV files")

    df = df.dropna(subset=["date", "value"])

    print(f"After cleaning: {len(df):,} records")

    write_delta("noaa_bcn", df, mode="overwrite")

    print("Delta conversion complete!")

except Exception as e:
    print(f"Error during Delta conversion: {e}")
    sys.exit(1)

finally:
    if conn:
        conn.close()