import requests
import json
import os
import sys
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaProducer


# ---------- CONFIG ----------
load_dotenv()
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_KEY")

CITY = "Barcelona"
OPENWEATHER_FOLDER = "../landing-zone/semi-structured/openweathermap"
# os.makedirs(OPENWEATHER_FOLDER, exist_ok=True)

print(f"Starting OpenWeatherMap collection for {CITY}...")

# ---------- KAFKA PRODUCER CONFIG ----------
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # your Kafka broker
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # serialize JSON
)

# ---------- SINGLE COLLECTION EXECUTION ----------
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

try:
    # Request weather data
    url_weather = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={OPENWEATHER_API_KEY}&units=metric"
    response = requests.get(url_weather)
    data = response.json()

    # --- Save locally ---
    filename_weather = f"{OPENWEATHER_FOLDER}/weather_{timestamp}.json"
    """
    with open(filename_weather, "w") as f:
        json.dump(data, f, indent=4)
    print(f"[OpenWeatherMap] Saved: {filename_weather}")
    """

    # --- Send to Kafka ---
    producer.send('weather-stream', value=data)
    producer.flush()  # ensure message is sent immediately
    print(f"[Kafka] Sent weather data for {CITY} to 'weather-stream' topic")

except Exception as e:
    print(f"[OpenWeatherMap] Error: {e}")

print("OpenWeatherMap collection complete!")