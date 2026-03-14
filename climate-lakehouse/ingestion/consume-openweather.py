import json
from kafka import KafkaConsumer
import sys
import os

# Add parent directory to path to import minio_client
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from landing_zone.minio_client import upload_to_bronze

print("Starting Kafka Consumer for OpenWeatherMap stream...")

consumer = KafkaConsumer(
    'weather-stream',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    weather_data = message.value
    print(f"Received data for {weather_data.get('name', 'Unknown City')} from Kafka.")
    
    # Store the semi-structured JSON in the MinIO Landing Zone
    upload_to_bronze(
        source_name="OpenWeatherMap",
        data_type="semi-structured",
        format_extension="json",
        data_content=json.dumps(weather_data, indent=4)
    )