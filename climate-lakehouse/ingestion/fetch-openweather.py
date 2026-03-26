import requests
import json
import os
import sys
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaProducer

sys.path.insert(0, "/opt/airflow/ingestion")
from delta_utils import s3, ensure_bucket, BUCKET_LANDING

load_dotenv("/opt/airflow/.env")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_KEY")

CITY = "Barcelona"
OPENWEATHER_FOLDER = "/opt/airflow/landing-zone/semi-structured/openweathermap"
os.makedirs(OPENWEATHER_FOLDER, exist_ok=True)

print(f"Starting OpenWeatherMap collection for {CITY}...")

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

ensure_bucket(BUCKET_LANDING)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

try:
    url = (
        f"https://api.openweathermap.org/data/2.5/weather"
        f"?q={CITY}&appid={OPENWEATHER_API_KEY}&units=metric"
    )
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()

    filename = f"{OPENWEATHER_FOLDER}/weather_{timestamp}.json"
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)
    print(f"[Local] Saved: {filename}")

    producer.send("weather-stream", value=data)
    producer.flush()
    print("[Kafka] Sent weather data")

    object_name = f"semi-structured/openweathermap/weather_{timestamp}.json"
    s3.upload_file(filename, BUCKET_LANDING, object_name,
                   ExtraArgs={"ContentType": "application/json"})
    print(f"[MinIO] Uploaded: {object_name}")

except Exception as e:
    print(f"[OpenWeatherMap] Error: {e}")
    raise

print("OpenWeatherMap collection complete!")