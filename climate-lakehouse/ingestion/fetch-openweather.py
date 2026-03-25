import requests
import json
import os
import io
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaProducer
from minio import Minio

# ---------- CONFIG ----------
# FIX: explicit .env path
load_dotenv("/opt/airflow/.env")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_KEY")

CITY = "Barcelona"
# FIX: absolute path
OPENWEATHER_FOLDER = "/opt/airflow/landing-zone/semi-structured/openweathermap"
os.makedirs(OPENWEATHER_FOLDER, exist_ok=True)

print(f"Starting OpenWeatherMap collection for {CITY}...")

# ---------- KAFKA PRODUCER ----------
producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

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
    url = (
        f"https://api.openweathermap.org/data/2.5/weather"
        f"?q={CITY}&appid={OPENWEATHER_API_KEY}&units=metric"
    )
    response = requests.get(url, timeout=30)
    response.raise_for_status()  # FIX: raises on 4xx/5xx automatically
    data = response.json()

    # ---- Save locally ----
    filename = f"{OPENWEATHER_FOLDER}/weather_{timestamp}.json"
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)
    print(f"[Local] Saved: {filename}")

    # ---- Send to Kafka ----
    producer.send("weather-stream", value=data)
    producer.flush()
    print("[Kafka] Sent weather data")

    # ---- Upload to MinIO ----
    data_bytes = json.dumps(data).encode("utf-8")
    object_name = f"semi-structured/openweathermap/weather_{timestamp}.json"

    minio_client.put_object(
        BUCKET_NAME,
        object_name,
        io.BytesIO(data_bytes),
        length=len(data_bytes),
        content_type="application/json"
    )
    print(f"[MinIO] Uploaded object: {object_name}")

except Exception as e:
    print(f"[OpenWeatherMap] Error: {e}")
    raise  # FIX: re-raise so Airflow marks the task as failed

print("OpenWeatherMap collection complete!")