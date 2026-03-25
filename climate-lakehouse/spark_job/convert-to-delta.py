from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp
import sys

print("Starting Delta Lake conversion for NOAA data...")

# FIX: do NOT use .config("spark.jars.packages", ...) here when submitting via spark-submit
# with --packages flag. It would cause a conflict. The packages are handled by spark-submit.
spark = SparkSession.builder \
    .appName("DeltaLakeConversion") \
    .getOrCreate()

# FIX: these paths must be accessible from the Spark container.
# The spark_job/ folder is mounted at /opt/spark_jobs/ in the Spark containers.
# The landing-zone is NOT mounted into Spark in the current docker-compose —
# so we use a MinIO path via S3A, or mount the folder.
# For simplicity here we assume landing-zone is also mounted (add this to docker-compose
# spark volumes: ./landing-zone:/opt/airflow/landing-zone)
CSV_PATH = "/opt/airflow/landing-zone/structured/noaa/"
DELTA_PATH = "/opt/airflow/landing-zone/structured/noaa_delta/"

try:
    # Read the raw CSV files
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(CSV_PATH)

    # Basic type casting
    df = df.withColumn("date", to_timestamp(col("date"))) \
           .withColumn("value", col("value").cast("double"))

    print(f"Read {df.count()} records from {CSV_PATH}")
    df.printSchema()

    # Write as Delta Table
    df.write.format("delta").mode("overwrite").save(DELTA_PATH)
    print(f"Success! Delta table written to {DELTA_PATH}")

except Exception as e:
    print(f"Error during Delta conversion: {e}")
    sys.exit(1)  # FIX: exit with error code so Airflow detects the failure

finally:
    spark.stop()