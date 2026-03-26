import requests
import os
import sys
from datetime import datetime
from dotenv import load_dotenv

sys.path.insert(0, "/opt/airflow/ingestion")
from delta_utils import s3, ensure_bucket, BUCKET_LANDING

load_dotenv("/opt/airflow/.env")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_KEY")

SAT_FOLDER = "/opt/airflow/landing-zone/unstructured/satellite"
os.makedirs(SAT_FOLDER, exist_ok=True)

print("Starting satellite ingestion...")

ensure_bucket(BUCKET_LANDING)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

try:
    url = f"https://tile.openweathermap.org/map/temp_new/5/15/12.png?appid={OPENWEATHER_API_KEY}"
    response = requests.get(url, timeout=30)

    if response.status_code == 200:
        filename = f"{SAT_FOLDER}/spain_temp_{timestamp}.png"
        with open(filename, "wb") as f:
            f.write(response.content)
        print(f"[Local] Saved: {filename}")

        object_name = f"unstructured/satellite/spain_temp_{timestamp}.png"
        s3.upload_file(filename, BUCKET_LANDING, object_name,
                       ExtraArgs={"ContentType": "image/png"})
        print(f"[MinIO] Uploaded: {object_name}")
    else:
        raise Exception(f"HTTP Error {response.status_code}")

except Exception as e:
    print(f"[Satellite] Error: {e}")
    raise

print("Satellite collection complete!")