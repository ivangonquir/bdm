"""

Consumes weather events from the Kafka topic 'weather-stream'
(produced by fetch-openweather.py) and appends them to the
Delta Lake table s3://delta/weather_stream in MinIO.

"""

import json
import sys
from datetime import datetime, timezone

import pyarrow as pa
from kafka import KafkaConsumer

sys.path.insert(0, "/opt/airflow/ingestion")
from delta_utils import write_delta, ensure_bucket

KAFKA_BROKERS = "kafka:9092"
KAFKA_TOPIC   = "weather-stream"
KAFKA_GROUP   = "airflow-weather-consumer"

print(f"Starting Kafka consumer — topic: {KAFKA_TOPIC}")

# Ensure the delta bucket exists in MinIO
ensure_bucket("delta")

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKERS,
    auto_offset_reset="earliest",  
    enable_auto_commit=False,       
    group_id=KAFKA_GROUP,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    consumer_timeout_ms=5_000,      
)

raw_records = [msg.value for msg in consumer]

if not raw_records:
    print("No new messages in Kafka — nothing to write.")
    consumer.close()
    sys.exit(0)

print(f"Consumed {len(raw_records)} messages from Kafka")


def safe_get(record, *keys, default=None):
    """Safely navigate nested dicts/lists."""
    val = record
    for k in keys:
        if isinstance(val, dict):
            val = val.get(k, default)
        elif isinstance(val, list) and isinstance(k, int):
            val = val[k] if len(val) > k else default
        else:
            return default
    return val if val is not None else default


table = pa.table({
    "ingested_at": pa.array(
        [datetime.now(tz=timezone.utc)] * len(raw_records),
        type=pa.timestamp("us", tz="UTC"),
    ),
    "event_ts": pa.array(
        [datetime.fromtimestamp(safe_get(r, "dt", default=0), tz=timezone.utc)
         for r in raw_records],
        type=pa.timestamp("us", tz="UTC"),
    ),
    "city":          pa.array([safe_get(r, "name",                     default="") for r in raw_records], type=pa.string()),
    "temp_c":        pa.array([safe_get(r, "main", "temp",             default=None) for r in raw_records], type=pa.float64()),
    "feels_like_c":  pa.array([safe_get(r, "main", "feels_like",       default=None) for r in raw_records], type=pa.float64()),
    "humidity_pct":  pa.array([safe_get(r, "main", "humidity",         default=None) for r in raw_records], type=pa.float64()),
    "pressure_hpa":  pa.array([safe_get(r, "main", "pressure",         default=None) for r in raw_records], type=pa.float64()),
    "wind_speed_ms": pa.array([safe_get(r, "wind", "speed",            default=None) for r in raw_records], type=pa.float64()),
    "weather_main":  pa.array([safe_get(r, "weather", 0, "main",       default="") for r in raw_records], type=pa.string()),
    "weather_desc":  pa.array([safe_get(r, "weather", 0, "description",default="") for r in raw_records], type=pa.string()),
})

write_delta("weather_stream", table, mode="append")

consumer.commit()
consumer.close()

print(f"Done — {len(raw_records)} weather events written to Delta.")