import requests
import os
import io
from datetime import datetime
from dotenv import load_dotenv
from minio import Minio

# FIX: explicit .env path
load_dotenv("/opt/airflow/.env")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_KEY")

# FIX: absolute path
SAT_FOLDER = "/opt/airflow/landing-zone/unstructured/satellite"
os.makedirs(SAT_FOLDER, exist_ok=True)

print("Starting satellite ingestion...")

# ---------- MINIO ----------
minio_client = Minio(
    "minio:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

BUCKET_NAME = "landing-zone"

if not minio_client.bucket_exists(BUCKET_NAME):
    minio_client.make_bucket(BUCKET_NAME)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

try:
    url = f"https://tile.openweathermap.org/map/temp_new/5/15/12.png?appid={OPENWEATHER_API_KEY}"
    response = requests.get(url, timeout=30)

    if response.status_code == 200:

        filename = f"{SAT_FOLDER}/spain_temp_{timestamp}.png"
        with open(filename, "wb") as f:
            f.write(response.content)
        print(f"[Local] Saved: {filename}")

        img_bytes = response.content
        object_name = f"unstructured/satellite/spain_temp_{timestamp}.png"

        minio_client.put_object(
            BUCKET_NAME,
            object_name,
            io.BytesIO(img_bytes),
            length=len(img_bytes),
            content_type="image/png"
        )
        print(f"[MinIO] Uploaded object: {object_name}")

    else:
        raise Exception(f"HTTP Error {response.status_code}")  # FIX: fail loudly

except Exception as e:
    print(f"[Satellite] Error: {e}")
    raise

print("Satellite collection complete!")