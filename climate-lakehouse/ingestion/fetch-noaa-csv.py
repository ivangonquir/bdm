import requests
import os
import csv
import time
import io
from datetime import datetime
from minio import Minio
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("NOAA_TOKEN")

DATASET_ID = "GHCND"
STATION_ID = "GHCND:SP000008181"

START_YEAR = 2000
END_YEAR = datetime.now().year

FOLDER = "../landing-zone/structured/noaa"
os.makedirs(FOLDER, exist_ok=True)

# ---------- MINIO ----------
minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

BUCKET_NAME = "landing-zone"

url = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
headers = {"token": TOKEN}

print(f"Starting NOAA ingestion {START_YEAR}-{END_YEAR}")

for year in range(START_YEAR, END_YEAR + 1):

    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    filename = f"{FOLDER}/noaa_bcn_{year}.csv"

    params = {
        "datasetid": DATASET_ID,
        "stationid": STATION_ID,
        "startdate": start_date,
        "enddate": end_date,
        "units": "metric",
        "limit": 1000,
        "datatypeid": "TMIN,TMAX,TAVG",
    }

    print(f"Fetching {year}...")

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:

        data = response.json()

        if "results" in data:

            with open(filename, "w", newline="", encoding="utf-8") as f:

                writer = csv.writer(f)

                writer.writerow(
                    ["date", "datatype", "station", "value", "attributes"]
                )

                for item in data["results"]:
                    writer.writerow([
                        item.get("date"),
                        item.get("datatype"),
                        item.get("station"),
                        item.get("value"),
                        item.get("attributes")
                    ])

            print(f"[Local] Saved {filename}")

            # ---- Upload to MinIO ----
            with open(filename, "rb") as f:
                data_bytes = f.read()

            object_name = f"structured/noaa/noaa_bcn_{year}.csv"

            minio_client.put_object(
                BUCKET_NAME,
                object_name,
                io.BytesIO(data_bytes),
                length=len(data_bytes),
                content_type="text/csv"
            )

            print(f"[MinIO] Uploaded {object_name}")

    else:
        print(f"API Error {response.status_code}")

    time.sleep(1)

print("NOAA ingestion complete!")