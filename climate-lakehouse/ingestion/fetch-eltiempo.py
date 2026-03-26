import requests
import os
import sys
from datetime import datetime
from dotenv import load_dotenv

sys.path.insert(0, "/opt/airflow/ingestion")
from delta_utils import s3, ensure_bucket, BUCKET_LANDING

load_dotenv("/opt/airflow/.env")

CITY = "Barcelona"
ELTIEMPO_FOLDER = "/opt/airflow/landing-zone/unstructured/eltiempo"
os.makedirs(ELTIEMPO_FOLDER, exist_ok=True)

print(f"Starting ElTiempo.es scraping for {CITY}...")

ensure_bucket(BUCKET_LANDING)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

try:
    url = f"https://www.eltiempo.es/{CITY.lower()}.html"
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)

    if response.status_code == 200:
        filename = f"{ELTIEMPO_FOLDER}/eltiempo_{timestamp}.html"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(response.text)
        print(f"[Local] Saved: {filename}")

        object_name = f"unstructured/eltiempo/eltiempo_{timestamp}.html"
        s3.upload_file(filename, BUCKET_LANDING, object_name,
                       ExtraArgs={"ContentType": "text/html"})
        print(f"[MinIO] Uploaded: {object_name}")
    else:
        raise Exception(f"HTTP Error {response.status_code}")

except Exception as e:
    print(f"[ElTiempo] Error: {e}")
    raise

print("ElTiempo collection complete!")