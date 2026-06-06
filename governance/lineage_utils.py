"""
Data Governance: Lineage Tracking.

Records data movement between zones into MongoDB governance.lineage.

Each lineage document captures:
  - task_id       : name of the script / Airflow task
  - run_ts        : UTC timestamp of the run
  - source        : input (zone + store + table/collection/path)
  - destination   : output (zone + store + table/collection/path)
  - rows_in       : records read from the source
  - rows_out      : records written to the destination
  - status        : "success" or "failed"
  - error         : error message if status == "failed"

Usage from any pipeline script:
    from lineage_utils import log_lineage

    log_lineage(
        task_id     = "clean_noaa",
        source      = {"zone": "landing",  "store": "minio/delta", "table": "noaa_bcn"},
        destination = {"zone": "trusted",  "store": "clickhouse",  "table": "trusted.noaa_bcn"},
        rows_in     = 42000,
        rows_out    = 41800,
    )
"""

from datetime import datetime, timezone
from pymongo import MongoClient

MONGO_URI = "mongodb://mongodb:27017/"


def log_lineage(
    task_id: str,
    source: dict,
    destination: dict,
    rows_in: int,
    rows_out: int,
    status: str = "success",
    error: str = "",
) -> None:
    """Insert one lineage record into MongoDB governance.lineage."""
    doc = {
        "task_id":     task_id,
        "run_ts":      datetime.now(timezone.utc),
        "source":      source,
        "destination": destination,
        "rows_in":     rows_in,
        "rows_out":    rows_out,
        "status":      status,
        "error":       error,
    }
    client = MongoClient(MONGO_URI)
    client["governance"]["lineage"].insert_one(doc)
    client.close()
    tag = "✓" if status == "success" else "✗"
    print(f"[Lineage {tag}] {task_id} | in={rows_in:,} out={rows_out:,} | "
          f"{source.get('table','?')} → {destination.get('table','?')}")