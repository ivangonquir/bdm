import requests
import os
import csv
import time
import sys
from datetime import datetime
from dotenv import load_dotenv

sys.path.insert(0, "/opt/airflow/ingestion")
from delta_utils import s3, ensure_bucket, BUCKET_LANDING

load_dotenv("/opt/airflow/.env")

TOKEN      = os.getenv("NOAA_TOKEN")
DATASET_ID = "GHCND"
STATION_ID = "GHCND:SP000008181"
START_YEAR = 1924
END_YEAR   = datetime.now().year

FOLDER = "/opt/airflow/landing-zone/structured/noaa"
os.makedirs(FOLDER, exist_ok=True)

ensure_bucket(BUCKET_LANDING)

url     = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
headers = {"token": TOKEN}

print(f"Starting NOAA ingestion {START_YEAR}-{END_YEAR}")

skipped = 0
fetched = 0
errors  = 0

for year in range(START_YEAR, END_YEAR + 1):
    filename    = f"{FOLDER}/noaa_bcn_{year}.csv"
    object_name = f"structured/noaa/noaa_bcn_{year}.csv"

    # ── Skip years already in MinIO (more reliable than checking local disk) ──
    try:
        s3.head_object(Bucket=BUCKET_LANDING, Key=object_name)
        print(f"[Skip] {year} already in MinIO — skipping.")
        skipped += 1
        continue
    except Exception:
        pass  # Not found in MinIO, proceed with fetch

    params = [
        ("datasetid",  DATASET_ID),
        ("stationid",  STATION_ID),
        ("startdate",  f"{year}-01-01"),
        ("enddate",    f"{year}-12-31"),
        ("units",      "metric"),
        ("limit",      1000),
        ("datatypeid", "TMIN"),
        ("datatypeid", "TMAX"),
        ("datatypeid", "TAVG"),
    ]

    print(f"Fetching {year}...")

    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
    except requests.exceptions.RequestException as e:
        print(f"[WARN] Network error for {year}: {e} — skipping.")
        errors += 1
        continue

    if response.status_code == 200:
        data = response.json()
        if "results" in data:
            with open(filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["date", "datatype", "station", "value", "attributes"])
                for item in data["results"]:
                    writer.writerow([
                        item.get("date"), item.get("datatype"),
                        item.get("station"), item.get("value"),
                        item.get("attributes"),
                    ])
            print(f"[Local] Saved {filename}")

            s3.upload_file(filename, BUCKET_LANDING, object_name,
                           ExtraArgs={"ContentType": "text/csv"})
            print(f"[MinIO] Uploaded {object_name}")
            fetched += 1
        else:
            print(f"[NOAA] No results for {year} — skipping.")

    elif response.status_code == 429:
        # Rate limited — wait longer and retry once
        print(f"[WARN] Rate limited on {year} (429) — waiting 60 s and retrying...")
        time.sleep(60)
        retry = requests.get(url, headers=headers, params=params, timeout=30)
        if retry.status_code == 200 and "results" in retry.json():
            data = retry.json()
            with open(filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["date", "datatype", "station", "value", "attributes"])
                for item in data["results"]:
                    writer.writerow([
                        item.get("date"), item.get("datatype"),
                        item.get("station"), item.get("value"),
                        item.get("attributes"),
                    ])
            s3.upload_file(filename, BUCKET_LANDING, object_name,
                           ExtraArgs={"ContentType": "text/csv"})
            print(f"[MinIO] Uploaded {object_name} (after retry)")
            fetched += 1
        else:
            print(f"[WARN] Retry failed for {year} — skipping.")
            errors += 1

    else:
        print(f"[WARN] NOAA API error {response.status_code} for {year} — skipping.")
        errors += 1

    time.sleep(1)

print(f"NOAA ingestion complete! fetched={fetched}, skipped={skipped}, errors={errors}")
if errors > 0:
    print(f"[WARN] {errors} year(s) failed — check logs above.")