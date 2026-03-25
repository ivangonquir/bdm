CLIMATE LAKEHOUSE PROJECT
README.txt

----------------------------------------------------------------------

FOLDER STRUCTURE

climate-lakehouse/
│
├─ airflow/
│   └─ dags/
│       └─ ingestion-dag.py        (Airflow DAG definition)
│
├─ ingestion/
│   ├─ fetch-openweather.py
│   ├─ fetch-eltiempo.py
│   ├─ fetch-noaa-csv.py
│   └─ fetch-satellite.py
│
├─ landing-zone/
│   ├─ structured/
│   │   ├─ noaa/                   (NOAA CSV datasets)
│   │   └─ noaa_delta/             (Delta Lake table)
│   ├─ semi-structured/            (OpenWeather JSON data)
│   └─ unstructured/               (HTML pages and satellite images)
│
├─ spark_job/
│   └─ convert-to-delta.py        (PySpark Delta conversion job)
│
├─ .env                            (API keys - never commit this)
├─ .gitignore
├─ docker-compose.yml
├─ requirements.txt
└─ README.txt

----------------------------------------------------------------------

PROJECT DESCRIPTION

This project implements a Climate Data Lakehouse with a full ingestion
layer, orchestration, object storage, stream processing, and open table
format support.

Data sources used:

- NOAA Climate Data (structured CSV, 2000-present)
- OpenWeatherMap API (semi-structured JSON)
- ElTiempo website scraping (unstructured HTML)
- OpenWeather satellite temperature tiles (unstructured images)

Streaming ingestion is implemented using Kafka.
Object storage is implemented using MinIO.
Orchestration is implemented using Apache Airflow.
Open Table Format is implemented using Delta Lake on Apache Spark.

----------------------------------------------------------------------

REQUIREMENTS

- Python 3.10+
- Docker Desktop
- OpenWeatherMap API key
- NOAA API token

Install Python dependencies:

    pip install -r requirements.txt

----------------------------------------------------------------------

INFRASTRUCTURE COMPONENTS

Docker Compose starts the following services:

1. Zookeeper
   Manages Kafka metadata and cluster coordination.

2. Kafka
   Receives streaming weather data from OpenWeatherMap.
   Configured with a healthcheck to ensure readiness before Airflow starts.

3. MinIO
   S3-compatible object storage used as the Data Lake landing zone.
   Console available at http://localhost:9001

4. Postgres
   Backend database for Airflow metadata.
   Required for LocalExecutor and multi-container Airflow setup.

5. Airflow Init
   One-shot container that runs db migrate and creates the admin user,
   then exits. Ensures the DB is ready before webserver and scheduler start.

6. Airflow Webserver
   Web UI for managing and monitoring DAGs.
   Available at http://localhost:8081 (admin / admin)

7. Airflow Scheduler
   Triggers DAG runs on schedule.

8. Spark Master
   Apache Spark cluster master node.
   UI available at http://localhost:8080

9. Spark Worker
   Executes Spark jobs submitted by the master.

----------------------------------------------------------------------

SETUP INSTRUCTIONS

1. Configure API Keys

Create a .env file in the project root directory:

    OPENWEATHER_KEY=YOUR_OPENWEATHER_API_KEY
    NOAA_TOKEN=YOUR_NOAA_API_TOKEN

Replace the placeholders with your actual API keys.

----------------------------------------------------------------------

2. Start Docker Services

Make sure Docker Desktop is running.

Open a terminal in the project root folder and run:

    docker compose up -d

Boot order is managed automatically via healthchecks and depends_on:

    postgres → airflow-init → airflow-webserver → airflow-scheduler
    zookeeper → kafka
    minio
    spark → spark-worker

Wait ~2 minutes for all services to become healthy on first boot.

Check running containers:

    docker ps

----------------------------------------------------------------------

3. Access Service UIs

    Airflow:  http://localhost:8081   (admin / admin)
    MinIO:    http://localhost:9001   (minioadmin / minioadmin)
    Spark:    http://localhost:8080

----------------------------------------------------------------------

4. Create Kafka Topic

Create the topic used for weather streaming data:

    docker exec -it climate-lakehouse-kafka-1 kafka-topics \
        --create --topic weather-stream \
        --bootstrap-server localhost:9092 \
        --partitions 1 --replication-factor 1

Verify the topic exists:

    docker exec -it climate-lakehouse-kafka-1 kafka-topics \
        --list --bootstrap-server localhost:9092

----------------------------------------------------------------------

5. Run the Airflow DAG

Open http://localhost:8081, log in with admin/admin.

- Go to DAGs → climate_ingestion_pipeline
- Toggle the DAG ON using the slider
- Click the Trigger DAG button (play icon)

The DAG runs these tasks:

    fetch_noaa         → downloads NOAA CSV files (2000-present)
    fetch_openweather  → fetches current weather JSON + sends to Kafka
    fetch_eltiempo     → scrapes ElTiempo HTML page
    fetch_satellite    → downloads OpenWeather temperature tile image

    fetch_noaa triggers convert_to_delta after completion.

All ingested files are saved locally under landing-zone/ and
uploaded to the MinIO landing-zone bucket automatically.

----------------------------------------------------------------------

6. Run the Delta Lake Conversion (Spark)

After NOAA CSVs are ingested, convert them to Delta format by running:

    docker exec spark /opt/spark/bin/spark-submit \
        --packages io.delta:delta-spark_2.12:3.1.0 \
        --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
        --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
        /opt/spark_jobs/convert-to-delta.py

Output will appear at:

    landing-zone/structured/noaa_delta/

Verify it succeeded:

    docker exec spark ls /opt/airflow/landing-zone/structured/noaa_delta/

You should see a _delta_log/ folder and multiple .snappy.parquet files.

----------------------------------------------------------------------

7. Data Storage Structure

Inside MinIO (landing-zone bucket):

landing-zone/
│
├─ structured/
│   └─ noaa/
│       └─ noaa_bcn_YEAR.csv          (one file per year, 2000-present)
│
├─ semi-structured/
│   └─ openweathermap/
│       └─ weather_TIMESTAMP.json
│
└─ unstructured/
    ├─ eltiempo/
    │   └─ eltiempo_TIMESTAMP.html
    │
    └─ satellite/
        └─ spain_temp_TIMESTAMP.png

Locally, a Delta table is also created at:

    landing-zone/structured/noaa_delta/

----------------------------------------------------------------------

8. Streaming Data (Kafka)

The OpenWeatherMap script sends weather data to Kafka in real time.

Topic used: weather-stream

This enables real-time ingestion for future streaming processing pipelines.

----------------------------------------------------------------------

9. Stop Docker Services

    docker compose down

This stops all containers. Your local files and MinIO data are preserved.

To also remove volumes (full reset):

    docker compose down -v

----------------------------------------------------------------------

NOTES

- Do NOT commit the .env file. It contains sensitive API keys.
- Do NOT commit the landing-zone/ folder. Data files can be large.
- The Airflow image uses Python 3.8. All ingestion dependencies
  (minio, kafka-python, python-dotenv, requests) are installed at
  container startup via the docker-compose command.
- Delta Lake requires Spark 3.x. The project uses apache/spark:3.5.3.
- spark-submit is located at /opt/spark/bin/spark-submit inside the
  Spark container (not on $PATH by default).
- On first boot, allow ~2 minutes for airflow-init to complete before
  the webserver becomes available.

----------------------------------------------------------------------