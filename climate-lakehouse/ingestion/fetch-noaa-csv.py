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
START_YEAR = 2000
END_YEAR   = datetime.now().year

FOLDER = "/opt/airflow/landing-zone/structured/noaa"
os.makedirs(FOLDER, exist_ok=True)

ensure_bucket(BUCKET_LANDING)

url     = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
headers = {"token": TOKEN}

print(f"Starting NOAA ingestion {START_YEAR}-{END_YEAR}")

for year in range(START_YEAR, END_YEAR + 1):
    filename = f"{FOLDER}/noaa_bcn_{year}.csv"

    # NOAA API requires repeated params for multiple datatypeids
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
    response = requests.get(url, headers=headers, params=params, timeout=30)

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

            object_name = f"structured/noaa/noaa_bcn_{year}.csv"
            s3.upload_file(filename, BUCKET_LANDING, object_name,
                           ExtraArgs={"ContentType": "text/csv"})
            print(f"[MinIO] Uploaded {object_name}")
        else:
            print(f"[NOAA] No results for {year}, skipping.")
    else:
        raise Exception(f"NOAA API error {response.status_code} for {year}")

    time.sleep(1)

print("NOAA ingestion complete!")