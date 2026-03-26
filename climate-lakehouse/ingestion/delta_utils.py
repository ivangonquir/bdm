"""
delta_utils.py
──────────────
Shared helpers for Delta Lake + MinIO operations.
Place this file in the ingestion/ folder.
"""

import os
from typing import Optional, Any

import boto3
import pyarrow as pa
from botocore.client import Config
from deltalake import DeltaTable, write_deltalake

# ── MinIO connection ──────────────────────────────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# Used by deltalake library for all S3 operations
storage_options: dict = {
    "endpoint_url":               MINIO_ENDPOINT,
    "access_key_id":              MINIO_ACCESS_KEY,
    "secret_access_key":          MINIO_SECRET_KEY,
    "region":                     "us-east-1",
    "allow_http":                 "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_ALLOW_HTTP":             "true",
}

# Used by boto3 for bucket/object operations
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

# ── Bucket constants ──────────────────────────────────────────────────────────
BUCKET_LANDING = "landing-zone"
BUCKET_DELTA   = "s3://delta"


def ensure_bucket(bucket_name: str) -> None:
    """Create a MinIO bucket if it does not already exist."""
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if bucket_name not in existing:
        s3.create_bucket(Bucket=bucket_name)
        print(f"[MinIO] Created bucket: {bucket_name}")


def delta_path(table_name: str) -> str:
    """Return the S3 URI for a Delta table stored in MinIO."""
    return f"{BUCKET_DELTA}/{table_name}"


# ── Write helpers ─────────────────────────────────────────────────────────────

def write_delta(
    table_name: str,
    data: Any,
    mode: str = "append",
    partition_by: Optional[list] = None,
    schema_mode: Optional[str] = "merge",
) -> None:
    """Write a PyArrow Table or pandas DataFrame to a Delta table in MinIO."""
    if hasattr(data, "to_arrow"):
        data = data.to_arrow()
    elif not isinstance(data, pa.Table):
        data = pa.Table.from_pandas(data)

    write_deltalake(
        delta_path(table_name),
        data,
        mode=mode,
        partition_by=partition_by,
        schema_mode=schema_mode,
        storage_options=storage_options,
    )
    print(f"[Delta] Wrote {len(data):,} rows → {delta_path(table_name)} (mode={mode})")


# ── Read helpers ──────────────────────────────────────────────────────────────

def read_delta(table_name: str, version: Optional[int] = None) -> pa.Table:
    """Read a Delta table from MinIO as a PyArrow Table."""
    dt = DeltaTable(
        delta_path(table_name),
        storage_options=storage_options,
        version=version,
    )
    return dt.to_pyarrow_table()


def open_delta(table_name: str, version: Optional[int] = None) -> DeltaTable:
    """Return a raw DeltaTable object for full control (history, delete, etc.)."""
    return DeltaTable(
        delta_path(table_name),
        storage_options=storage_options,
        version=version,
    )


# ── Utility helpers ───────────────────────────────────────────────────────────

def table_history(table_name: str, limit: int = 20):
    """Return the commit history of a Delta table as a pandas DataFrame."""
    import pandas as pd
    return pd.DataFrame(open_delta(table_name).history(limit=limit))


def table_info(table_name: str) -> dict:
    """Return basic metadata about a Delta table."""
    dt = open_delta(table_name)
    return {
        "table":   table_name,
        "version": dt.version(),
        "files":   len(dt.files()),
        "schema":  {f.name: str(f.type) for f in dt.schema().fields},
    }