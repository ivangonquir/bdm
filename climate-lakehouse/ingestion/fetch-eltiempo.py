import requests
import os
import io
from datetime import datetime
from minio import Minio

# ---------- CONFIG ----------
CITY = "Barcelona"
# FIX: use absolute path that works inside the Airflow container
ELTIEMPO_FOLDER = "/opt/airflow/landing-zone/unstructured/eltiempo"
os.makedirs(ELTIEMPO_FOLDER, exist_ok=True)

print(f"Starting ElTiempo.es scraping for {CITY}...")

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
    url = f"https://www.eltiempo.es/{CITY.lower()}.html"
    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(url, headers=headers, timeout=30)

    if response.status_code == 200:

        # ---- Save locally ----
        filename = f"{ELTIEMPO_FOLDER}/eltiempo_{timestamp}.html"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(response.text)
        print(f"[Local] Saved: {filename}")

        # ---- Upload to MinIO ----
        html_bytes = response.text.encode("utf-8")
        object_name = f"unstructured/eltiempo/eltiempo_{timestamp}.html"

        minio_client.put_object(
            BUCKET_NAME,
            object_name,
            io.BytesIO(html_bytes),
            length=len(html_bytes),
            content_type="text/html"
        )
        print(f"[MinIO] Uploaded object: {object_name}")

    else:
        print(f"[ElTiempo] HTTP Error: {response.status_code}")
        raise Exception(f"HTTP Error {response.status_code}")

except Exception as e:
    print(f"[ElTiempo] Error: {e}")
    raise  # FIX: re-raise so Airflow marks the task as failed

print("ElTiempo collection complete!")