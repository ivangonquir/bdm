"""
Landing Zone → Trusted Zone: NOAA structured data (PySpark).

Reads the raw NOAA Delta table from MinIO via the S3A connector, applies
data-quality transformations using Spark DataFrames, and writes the result
to ClickHouse trusted.noaa_bcn.

Cleaning applied:
  - Drop rows with null date or value
  - Remove records outside Barcelona-specific bounds [-15, 45] °C
  - Deduplicate on (date, datatype, station)
  - Parse attributes field (,,E,) into m_flag, q_flag, s_flag columns
  - Flag TMIN >= TMAX inconsistencies with is_inconsistent
  - Detect and log missing days in the time series
"""

import clickhouse_connect
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PACKAGES = (
    "io.delta:delta-spark_2.12:3.2.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4"
)

spark = (
    SparkSession.builder
    .master("spark://spark-master:7077")
    .appName("clean_noaa")
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
    .config("spark.executorEnv.PYSPARK_PYTHON", "python3.11")
    .getOrCreate()
)

print("=== NOAA Trusted Zone Cleaning (Spark) ===")

# ── Read from Delta (MinIO) ──────────────────────────────────────────────────
df = spark.read.format("delta").load("s3a://delta/noaa_bcn")
print(f"Read {df.count():,} records from Delta landing zone")

# ── 1. Drop null date / value ────────────────────────────────────────────────
df = df.dropna(subset=["date", "value"])

# ── 2. Barcelona temperature range [-15, 45] °C ─────────────────────────────
df = df.filter((F.col("value") >= -15) & (F.col("value") <= 45))

# ── 3. Deduplicate on natural key ────────────────────────────────────────────
df = df.dropDuplicates(["date", "datatype", "station"])

# ── 4. Normalise date to DateType ────────────────────────────────────────────
df = df.withColumn("date", F.to_date(F.col("date")))
df = df.dropna(subset=["date"])

# ── 5. Parse attributes → m_flag, q_flag, s_flag ─────────────────────────────
df = df.withColumn(
    "attributes",
    F.coalesce(F.col("attributes").cast("string"), F.lit("")),
)
split_col = F.split(F.col("attributes"), ",")
df = (df
    .withColumn("m_flag", F.trim(F.coalesce(F.element_at(split_col, 1), F.lit(""))))
    .withColumn("q_flag", F.trim(F.coalesce(F.element_at(split_col, 2), F.lit(""))))
    .withColumn("s_flag", F.trim(F.coalesce(F.element_at(split_col, 3), F.lit(""))))
)

# ── 6. TMIN / TMAX consistency check ────────────────────────────────────────
tmin_df = (df.filter(F.col("datatype") == "TMIN")
    .select("date", "station", F.col("value").alias("tmin")))
tmax_df = (df.filter(F.col("datatype") == "TMAX")
    .select("date", "station", F.col("value").alias("tmax")))

inconsistent = (
    tmin_df.join(tmax_df, ["date", "station"])
    .filter(F.col("tmin") >= F.col("tmax"))
    .select("date", "station", F.lit(True).alias("is_inconsistent"))
)
n_inconsistent = inconsistent.count()
print(f"Flagged {n_inconsistent:,} dates with TMIN >= TMAX")

df = df.join(inconsistent, ["date", "station"], "left")
df = df.withColumn(
    "is_inconsistent",
    F.coalesce(F.col("is_inconsistent"), F.lit(False)),
)

# ── 7. Missing day detection (log only) ──────────────────────────────────────
bounds = df.agg(F.min("date").alias("mn"), F.max("date").alias("mx")).collect()[0]
if bounds["mn"] and bounds["mx"]:
    span = (bounds["mx"] - bounds["mn"]).days + 1
    present = df.select("date").distinct().count()
    print(f"Detected {span - present:,} missing days in the time series")

final_count = df.count()
print(f"Final clean record count: {final_count:,}")

# ── Write to ClickHouse ──────────────────────────────────────────────────────
pdf = df.select(
    "date", "datatype", "station", "value", "attributes",
    "m_flag", "q_flag", "s_flag", "is_inconsistent",
).toPandas()

pdf["date"] = pdf["date"].apply(
    lambda d: d if hasattr(d, "year") else __import__("pandas").Timestamp(d).date()
)
pdf["is_inconsistent"] = pdf["is_inconsistent"].astype("uint8")

ch = clickhouse_connect.get_client(host="clickhouse", port=8123)
ch.command("CREATE DATABASE IF NOT EXISTS trusted")
ch.command("DROP TABLE IF EXISTS trusted.noaa_bcn")
ch.command("""
    CREATE TABLE trusted.noaa_bcn (
        date             Date32,
        datatype         LowCardinality(String),
        station          String,
        value            Nullable(Float64),
        attributes       String,
        m_flag           String,
        q_flag           String,
        s_flag           String,
        is_inconsistent  UInt8
    ) ENGINE = MergeTree()
    ORDER BY (date, datatype, station)
""")
ch.insert_df(
    "trusted.noaa_bcn",
    pdf[["date", "datatype", "station", "value", "attributes",
         "m_flag", "q_flag", "s_flag", "is_inconsistent"]],
)
print(f"[ClickHouse] Wrote {len(pdf):,} rows → trusted.noaa_bcn")

spark.stop()
