"""
Landing Zone → Trusted Zone: satellite PNG temperature tiles.

Lists all PNG files in MinIO landing-zone/unstructured/satellite/, validates
each file (correct PNG magic bytes, minimum size) and copies valid files to
MinIO trusted-zone/unstructured/satellite/.

Validation applied:
  - First 8 bytes must match the PNG signature
  - File must be at least 1 KB (filters out empty/truncated downloads)
"""

import sys

sys.path.insert(0, "/opt/airflow/ingestion")
from delta_utils import s3, ensure_bucket

BUCKET_LANDING = "landing-zone"
BUCKET_TRUSTED = "trusted-zone"
PREFIX         = "unstructured/satellite/"
PNG_SIGNATURE  = b"\x89PNG\r\n\x1a\n"

print("=== Satellite Trusted Zone Cleaning ===")

ensure_bucket(BUCKET_TRUSTED)

# ── List PNG files in landing zone ───────────────────────────────────────────
response = s3.list_objects_v2(Bucket=BUCKET_LANDING, Prefix=PREFIX)
files    = [obj["Key"] for obj in response.get("Contents", [])
            if obj["Key"].endswith(".png")]

print(f"Found {len(files)} PNG files in landing zone")

valid   = 0
invalid = 0

for key in files:
    try:
        obj     = s3.get_object(Bucket=BUCKET_LANDING, Key=key)
        content = obj["Body"].read()

        # Validate PNG magic bytes
        if not content[:8] == PNG_SIGNATURE:
            print(f"[SKIP] {key} — invalid PNG signature")
            invalid += 1
            continue

        # Validate minimum size (a rendered temperature tile is always > 1 KB)
        if len(content) < 1024:
            print(f"[SKIP] {key} — too small ({len(content)} bytes)")
            invalid += 1
            continue

        s3.put_object(
            Bucket=BUCKET_TRUSTED,
            Key=key,
            Body=content,
            ContentType="image/png",
        )
        valid += 1

    except Exception as exc:
        print(f"[ERROR] {key}: {exc}")
        invalid += 1

print(f"Done — valid: {valid}, skipped/invalid: {invalid}")
