"""
Landing Zone → Trusted Zone: ElTiempo unstructured HTML files.

Lists all HTML files in MinIO landing-zone/unstructured/eltiempo/, validates
each file (non-empty, valid HTML structure, UTF-8 encoding) and copies valid
files to MinIO trusted-zone/unstructured/eltiempo/.

Validation applied:
  - File must be at least 100 bytes
  - Must contain an <html> tag (confirms it is an HTML document)
  - Content is re-encoded as UTF-8 to standardise encoding
"""

import sys

sys.path.insert(0, "/opt/airflow/ingestion")
from delta_utils import s3, ensure_bucket

BUCKET_LANDING = "landing-zone"
BUCKET_TRUSTED = "trusted-zone"
PREFIX         = "unstructured/eltiempo/"

print("=== ElTiempo Trusted Zone Cleaning ===")

ensure_bucket(BUCKET_TRUSTED)

# ── List HTML files in landing zone ─────────────────────────────────────────
response = s3.list_objects_v2(Bucket=BUCKET_LANDING, Prefix=PREFIX)
files    = [obj["Key"] for obj in response.get("Contents", [])
            if obj["Key"].endswith(".html")]

print(f"Found {len(files)} HTML files in landing zone")

valid   = 0
invalid = 0

for key in files:
    try:
        obj     = s3.get_object(Bucket=BUCKET_LANDING, Key=key)
        content = obj["Body"].read()

        # Validate minimum size
        if len(content) < 100:
            print(f"[SKIP] {key} — too small ({len(content)} bytes)")
            invalid += 1
            continue

        # Validate HTML structure
        text = content.decode("utf-8", errors="replace")
        if "<html" not in text.lower():
            print(f"[SKIP] {key} — no <html> tag found")
            invalid += 1
            continue

        # Re-encode as clean UTF-8
        clean = text.encode("utf-8")

        s3.put_object(
            Bucket=BUCKET_TRUSTED,
            Key=key,
            Body=clean,
            ContentType="text/html; charset=utf-8",
        )
        valid += 1

    except Exception as exc:
        print(f"[ERROR] {key}: {exc}")
        invalid += 1

print(f"Done — valid: {valid}, skipped/invalid: {invalid}")
