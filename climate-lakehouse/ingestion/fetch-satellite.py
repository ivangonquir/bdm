import requests
import os
import io
from datetime import datetime
from dotenv import load_dotenv
from minio import Minio

load_dotenv()
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_KEY")

SAT_FOLDER = "../landing-zone/unstructured/satellite"
os.makedirs(SAT_FOLDER, exist_ok=True)

print("Starting satellite ingestion...")

# ---------- MINIO ----------
minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

BUCKET_NAME = "landing-zone"

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

try:

    url = f"https://tile.openweathermap.org/map/temp_new/5/15/12.png?appid={OPENWEATHER_API_KEY}"

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:

        filename = f"{SAT_FOLDER}/spain_temp_{timestamp}.png"

        with open(filename, "wb") as f:
            f.write(response.content)

        print(f"[Local] Saved: {filename}")

        # ---- Upload to MinIO ----
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
        print(f"HTTP Error {response.status_code}")

except Exception as e:
    print(f"[Satellite] Error: {e}")

print("Satellite collection complete!")