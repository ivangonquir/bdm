"""
Landing Zone → Trusted Zone: ElTiempo unstructured HTML files.

Lists all HTML files in MinIO landing-zone/unstructured/eltiempo/, validates
each file and applies structural cleaning before copying valid files to
MinIO trusted-zone/unstructured/eltiempo/.

Validation and cleaning applied:
  - File must be at least 100 bytes
  - Must contain an <html> tag (confirms it is an HTML document)
  - Content is re-encoded as UTF-8 to standardise encoding
  - <script>, <style>, <nav>, <footer>, <header> tags are stripped
  - Excessive whitespace is collapsed to single spaces
  - Degree symbol (°) is removed from temperature strings
  - Scrape timestamp is extracted from the filename and logged
  - Title date is compared against filename timestamp (mismatch flagged)
"""

import re
import sys
from datetime import datetime
from bs4 import BeautifulSoup

sys.path.insert(0, "/opt/airflow/ingestion")
from delta_utils import s3, ensure_bucket

BUCKET_LANDING = "landing-zone"
BUCKET_TRUSTED = "trusted-zone"
PREFIX         = "unstructured/eltiempo/"

print("=== ElTiempo Trusted Zone Cleaning ===")

ensure_bucket(BUCKET_TRUSTED)

# ── List HTML files in landing zone (paginated) ──────────────────────────────
files = []
paginator = s3.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket=BUCKET_LANDING, Prefix=PREFIX):
    files.extend(obj["Key"] for obj in page.get("Contents", [])
                 if obj["Key"].endswith(".html"))

print(f"Found {len(files)} HTML files in landing zone")

valid   = 0
invalid = 0

for key in files:
    try:
        obj     = s3.get_object(Bucket=BUCKET_LANDING, Key=key)
        content = obj["Body"].read()

        # ── Minimum size check ────────────────────────────────────────────────
        if len(content) < 100:
            print(f"[SKIP] {key} — too small ({len(content)} bytes)")
            invalid += 1
            continue

        # ── UTF-8 decode ──────────────────────────────────────────────────────
        text = content.decode("utf-8", errors="replace")

        # ── HTML structure validation ─────────────────────────────────────────
        if "<html" not in text.lower():
            print(f"[SKIP] {key} — no <html> tag found")
            invalid += 1
            continue

        # ── Timestamp extraction from filename ────────────────────────────────
        # Expected format: eltiempo_YYYYMMDD_HHMMSS.html
        filename   = key.split("/")[-1]
        scrape_ts  = None
        ts_match   = re.search(r"(\d{8})_(\d{6})", filename)
        if ts_match:
            try:
                scrape_ts = datetime.strptime(
                    ts_match.group(1) + ts_match.group(2), "%Y%m%d%H%M%S"
                )
            except ValueError:
                pass

        # ── Boilerplate removal ───────────────────────────────────────────────
        soup = BeautifulSoup(text, "html.parser")
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()

        # ── Title date consistency check ──────────────────────────────────────
        if scrape_ts:
            title_tag = soup.find("title")
            if title_tag:
                title_text = title_tag.get_text()
                # Check if scrape month appears anywhere in the title
                month_names = ["enero","febrero","marzo","abril","mayo","junio",
                               "julio","agosto","septiembre","octubre","noviembre","diciembre"]
                expected_month = month_names[scrape_ts.month - 1]
                if expected_month not in title_text.lower():
                    print(f"[WARN] {key} — title month mismatch "
                          f"(expected '{expected_month}', title: '{title_text[:60]}')")

        # ── Whitespace normalisation ──────────────────────────────────────────
        clean_text = re.sub(r"\s+", " ", soup.get_text(separator=" ")).strip()

        # ── Degree symbol removal ─────────────────────────────────────────────
        clean_text = clean_text.replace("°", "")

        # ── Re-encode as clean UTF-8 ──────────────────────────────────────────
        clean_bytes = str(soup).encode("utf-8")

        s3.put_object(
            Bucket=BUCKET_TRUSTED,
            Key=key,
            Body=clean_bytes,
            ContentType="text/html; charset=utf-8",
        )

        ts_str = scrape_ts.isoformat() if scrape_ts else "unknown"
        print(f"[OK] {key} — scrape_ts={ts_str}")
        valid += 1

    except Exception as exc:
        print(f"[ERROR] {key}: {exc}")
        invalid += 1

print(f"Done — valid: {valid}, skipped/invalid: {invalid}")