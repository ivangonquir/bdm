"""
Landing Zone → Trusted Zone: OpenWeatherMap semi-structured data (PySpark).

Reads the raw weather_stream Delta table from MinIO via the S3A connector,
applies data-quality transformations using Spark DataFrames, and writes
cleaned documents to MongoDB trusted.weather_stream.

Cleaning applied:
  - Deduplicate on event_ts
  - Validate temperature range [-15, 45] °C
  - Validate humidity range [0, 100] %
  - Validate pressure range [900, 1100] hPa
  - Normalise UTC offset to string (e.g. "UTC+1")
  - Drop internal API metadata columns (base, cod)
  - Fill missing string fields with defaults
"""

import sys
import numpy as np
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PACKAGES = (
    "io.delta:delta-spark_2.12:3.2.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4"
)

spark = (
    SparkSession.builder
    .master("spark://spark-master:7077")
    .appName("clean_openweather")
    .config("spark.jars.packages", PACKAGES)
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.pyspark.python", "python3.11")
    .config("spark.executorEnv.PYSPARK_PYTHON", "python3.11")
    .getOrCreate()
)

print("=== OpenWeather Trusted Zone Cleaning (Spark) ===")

# ── Read from Delta (MinIO) ──────────────────────────────────────────────────
try:
    df = spark.read.format("delta").load("s3a://delta/weather_stream")
except Exception as e:
    print(f"weather_stream Delta table not found or empty — skipping. ({e})")
    spark.stop()
    sys.exit(0)

if df.count() == 0:
    print("No data to process — exiting.")
    spark.stop()
    sys.exit(0)

print(f"Read {df.count():,} records from Delta landing zone")

# ── 1. Deduplicate on event timestamp ────────────────────────────────────────
df = df.dropDuplicates(["event_ts"])

# ── 2. Temperature range [-15, 45] °C ───────────────────────────────────────
df = df.filter(
    F.col("temp_c").isNull()
    | ((F.col("temp_c") >= -15) & (F.col("temp_c") <= 45))
)

# ── 3. Humidity range [0, 100] % ─────────────────────────────────────────────
df = df.filter(
    F.col("humidity_pct").isNull()
    | ((F.col("humidity_pct") >= 0) & (F.col("humidity_pct") <= 100))
)

# ── 4. Pressure range [900, 1100] hPa ────────────────────────────────────────
df = df.filter(
    F.col("pressure_hpa").isNull()
    | ((F.col("pressure_hpa") >= 900) & (F.col("pressure_hpa") <= 1100))
)

# ── 5. UTC offset normalisation ──────────────────────────────────────────────
if "utc_offset" in df.columns:
    hours = (F.col("utc_offset").cast("long") / 3600).cast("int")
    df = df.withColumn(
        "utc_offset",
        F.when(F.col("utc_offset").isNull(), F.lit(""))
         .when(hours >= 0, F.concat(F.lit("UTC+"), hours.cast("string")))
         .otherwise(F.concat(F.lit("UTC"), hours.cast("string"))),
    )

# ── 6. Drop internal API metadata ────────────────────────────────────────────
for col in ("base", "cod"):
    if col in df.columns:
        df = df.drop(col)

# ── 7. Fill missing string fields ────────────────────────────────────────────
df = (df
    .withColumn("city",         F.coalesce(F.col("city"),         F.lit("Barcelona")))
    .withColumn("weather_main", F.coalesce(F.col("weather_main"), F.lit("")))
    .withColumn("weather_desc", F.coalesce(F.col("weather_desc"), F.lit("")))
)

print(f"Final clean record count: {df.count():,}")

# ── Convert to MongoDB-safe dicts ────────────────────────────────────────────
pdf = df.toPandas()

records = pdf.to_dict("records")
for i, r in enumerate(records):
    for key in ("ingested_at", "event_ts"):
        val = r.get(key)
        if hasattr(val, "to_pydatetime"):
            r[key] = val.to_pydatetime()
    records[i] = {k: (v.item() if isinstance(v, np.generic) else v) for k, v in r.items()}

# ── Write to MongoDB ─────────────────────────────────────────────────────────
client = MongoClient("mongodb://mongodb:27017/")
collection = client["trusted"]["weather_stream"]
collection.drop()
if records:
    collection.insert_many(records)
print(f"[MongoDB] Wrote {len(records)} documents → trusted.weather_stream")
client.close()

spark.stop()
