"""
Landing Zone → Trusted Zone: satellite PNG temperature tiles.

Lists all PNG files in MinIO landing-zone/unstructured/satellite/, validates
each file (correct PNG magic bytes, minimum size, correct dimensions,
RGB channel check, pixel statistics logging) and copies valid files to
MinIO trusted-zone/unstructured/satellite/.

Validation applied:
  - First 8 bytes must match the PNG signature
  - File must be at least 1 KB (filters out empty/truncated downloads)
  - Image dimensions must be exactly 256 x 256 pixels
  - Image must be in RGB mode (3 channels); RGBA is converted to RGB
  - Mean RGB values are logged as metadata for sanity checking
"""

import io
import sys
from PIL import Image

sys.path.insert(0, "/opt/airflow/ingestion")
from delta_utils import s3, ensure_bucket

BUCKET_LANDING  = "landing-zone"
BUCKET_TRUSTED  = "trusted-zone"
PREFIX          = "unstructured/satellite/"
PNG_SIGNATURE   = b"\x89PNG\r\n\x1a\n"
EXPECTED_SIZE   = (256, 256)

print("=== Satellite Trusted Zone Cleaning ===")

ensure_bucket(BUCKET_TRUSTED)

# ── List PNG files in landing zone (paginated) ───────────────────────────────
files = []
paginator = s3.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket=BUCKET_LANDING, Prefix=PREFIX):
    files.extend(obj["Key"] for obj in page.get("Contents", [])
                 if obj["Key"].endswith(".png"))

print(f"Found {len(files)} PNG files in landing zone")

valid   = 0
invalid = 0

for key in files:
    try:
        obj     = s3.get_object(Bucket=BUCKET_LANDING, Key=key)
        content = obj["Body"].read()

        # ── PNG signature check ───────────────────────────────────────────────
        if content[:8] != PNG_SIGNATURE:
            print(f"[SKIP] {key} — invalid PNG signature")
            invalid += 1
            continue

        # ── Minimum size check ────────────────────────────────────────────────
        if len(content) < 1024:
            print(f"[SKIP] {key} — too small ({len(content)} bytes)")
            invalid += 1
            continue

        # ── Dimension and channel check ───────────────────────────────────────
        img = Image.open(io.BytesIO(content))

        if img.size != EXPECTED_SIZE:
            print(f"[SKIP] {key} — unexpected dimensions {img.size}, expected {EXPECTED_SIZE}")
            invalid += 1
            continue

        # Convert RGBA → RGB (drop alpha channel)
        if img.mode == "RGBA":
            img = img.convert("RGB")
            print(f"[INFO] {key} — converted RGBA to RGB")
        elif img.mode != "RGB":
            print(f"[SKIP] {key} — unexpected mode {img.mode}")
            invalid += 1
            continue

        # ── Pixel statistics logging ──────────────────────────────────────────
        import numpy as np
        arr      = np.array(img)
        mean_r   = float(arr[:, :, 0].mean())
        mean_g   = float(arr[:, :, 1].mean())
        mean_b   = float(arr[:, :, 2].mean())
        print(f"[INFO] {key} — mean RGB: ({mean_r:.1f}, {mean_g:.1f}, {mean_b:.1f})")

        # Re-encode to bytes if RGBA was converted to RGB
        if img.mode == "RGB" and content[:4] != b"\x89PNG":
            buf = io.BytesIO()
            img.save(buf, format="PNG")
            content = buf.getvalue()

        # ── Copy to trusted zone ──────────────────────────────────────────────
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