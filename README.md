# Climate Lakehouse Project

## Folder Structure

```
climate-lakehouse/
│
├─ airflow/
│   └─ dags/
│       └─ ingestion-dag.py           # Airflow DAG definition
│
├─ ingestion/
│   ├─ delta_utils.py                 # shared MinIO + Delta helpers
│   ├─ fetch-noaa-csv.py
│   ├─ fetch-openweather.py
│   ├─ fetch-eltiempo.py
│   ├─ fetch-satellite.py
│   ├─ convert-to-delta.py            # DuckDB → Delta Lake conversion
│   └─ consume-weather-kafka.py       # Kafka consumer → Delta Lake
│
├─ landing-zone/
│   ├─ structured/
│   │   └─ noaa/                      # NOAA CSV datasets, one per year
│   ├─ semi-structured/
│   │   └─ openweathermap/            # OpenWeather JSON snapshots
│   └─ unstructured/
│       ├─ eltiempo/                  # scraped HTML pages
│       └─ satellite/                 # temperature tile images
│
├─ .env                               # API keys — never commit this
├─ .gitignore
├─ docker-compose.yml
├─ requirements.txt
└─ README.md
```

---

## Project Description

A Climate Data Lakehouse with a full ingestion layer, orchestration, object storage, stream processing, and open table format support.

| Concern | Technology |
|---|---|
| Data sources | NOAA CSV, OpenWeatherMap API, ElTiempo scraping, satellite tiles |
| Streaming ingestion | Apache Kafka |
| Object storage | MinIO (S3-compatible) |
| Orchestration | Apache Airflow |
| Open Table Format | Delta Lake via **delta-rs** (no JVM, no Spark) |
| Query engine | DuckDB (CSV → Delta conversion) |

---

## Requirements
 
- Python 3.11+
- Docker Desktop
- OpenWeatherMap API key
- NOAA API token
 
Install Python dependencies locally (optional, for running scripts outside Docker):
 
```bash
pip install -r requirements.txt
```

---
 
## Infrastructure Components
 
Docker Compose starts the following services:
 
| Service | Description |
|---|---|
| **Zookeeper** | Manages Kafka metadata and cluster coordination |
| **Kafka** | Receives streaming weather data from OpenWeatherMap |
| **MinIO** | S3-compatible object storage — console at http://localhost:9001 |
| **Postgres** | Metadata database for Airflow (required for LocalExecutor) |
| **airflow-init** | One-shot container: migrates DB + creates admin user, then exits |
| **airflow-webserver** | DAG management UI at http://localhost:8081 |
| **airflow-scheduler** | Triggers DAG runs on schedule |
 
> **Note:** Spark is **not** used in this project. Delta Lake is written using the `deltalake` Python library (delta-rs), which requires no JVM.
 
---

## Setup Instructions
 
### 1. Configure API Keys
 
Create a `.env` file in the project root:
 
```env
OPENWEATHER_KEY=YOUR_OPENWEATHER_API_KEY
NOAA_TOKEN=YOUR_NOAA_API_TOKEN
AIRFLOW__WEBSERVER__SECRET_KEY=YOUR_SECRET_KEY
```
 
> **Important:** `AIRFLOW__WEBSERVER__SECRET_KEY` must be set and identical across all Airflow containers (webserver, scheduler, init). Without it, each container generates a random key on startup causing 403 errors when fetching task logs in the UI.

### 2. Start Docker Services

Make sure Docker Desktop is running, then:

```bash
docker compose up -d
```

Boot order is managed automatically via healthchecks:

```
postgres → airflow-init → airflow-webserver → airflow-scheduler
zookeeper → kafka
minio
```

> Allow ~2 minutes on first boot for `airflow-init` to finish.

Check all containers are healthy:

```bash
docker ps
```

### 3. Access Service UIs

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8081 | admin / admin |
| MinIO | http://localhost:9001 | minioadmin / minioadmin |

### 4. Create Kafka Topic

Create the topic used for weather streaming before triggering the DAG:

```bash
docker exec -it climate-lakehouse-kafka-1 kafka-topics --create --topic weather-stream --bootstrap-server localhost:9092 \
--partitions 1 --replication-factor 1
```

Verify it exists:

```bash
docker exec -it climate-lakehouse-kafka-1 kafka-topics --list --bootstrap-server localhost:9092
```

### 5. Run the Airflow DAG
 
1. Open http://localhost:8081 and log in with `admin / admin`
2. Go to **DAGs** → `climate_ingestion_pipeline`
3. Toggle the DAG **ON** (slider on the left)
4. Click **Trigger DAG** (play button on the right)
 
> Expected run time: **~5–8 minutes** on the first run. Subsequent runs are faster as NOAA skips years already uploaded to MinIO.
 
The DAG task flow:
 
```
fetch_noaa ──────────────────► convert_noaa_to_delta
                                (DuckDB reads CSVs → writes s3://delta/noaa_bcn)
 
fetch_openweather ───────────► consume_weather_kafka
                                (Kafka consumer → writes s3://delta/weather_stream)
 
fetch_eltiempo ──┐
                 │  (parallel, writes metadata to s3://delta/eltiempo_metadata)
fetch_satellite ─┘  (parallel, writes metadata to s3://delta/satellite_metadata)
```
 
> To debug a failing task: click the **red square** in Grid view → **Logs**.
> If logs return a 403 error, make sure `AIRFLOW__WEBSERVER__SECRET_KEY` is set in your `.env`.
 
---

## Data Storage Structure
 
### MinIO `landing-zone` bucket (raw files)
 
```
landing-zone/
├─ structured/noaa/noaa_bcn_YEAR.csv
├─ semi-structured/openweathermap/weather_TIMESTAMP.json
└─ unstructured/
    ├─ eltiempo/eltiempo_TIMESTAMP.html
    └─ satellite/spain_temp_TIMESTAMP.png
```
 
### MinIO `delta` bucket (Delta Lake tables)
 
```
delta/
├─ noaa_bcn/                # NOAA temperature data (structured)
│   ├─ _delta_log/
│   └─ *.snappy.parquet
├─ weather_stream/          # OpenWeather Kafka stream (semi-structured)
│   ├─ _delta_log/
│   └─ *.snappy.parquet
├─ eltiempo_metadata/       # Ingestion metadata for ElTiempo HTML files
│   ├─ _delta_log/
│   └─ *.snappy.parquet
└─ satellite_metadata/      # Ingestion metadata for satellite PNG tiles
    ├─ _delta_log/
    └─ *.snappy.parquet
```
 
> **Structured data** (NOAA, OpenWeather) is stored as full Delta tables containing the data itself.
> **Unstructured data** (ElTiempo, satellite) is stored as raw files in MinIO. Delta tables track metadata only (filename, URL, size, ingestion timestamp).
 
---
 
## Streaming Data (Kafka)
 
`fetch-openweather.py` produces one message per DAG run to the `weather-stream` topic.
 
`consume-weather-kafka.py` consumes all unread messages and appends them to the Delta table `s3://delta/weather_stream`. Kafka offsets are committed **only after** a successful Delta write (at-least-once delivery).
 
---
 
## Delta Lake (Open Table Format)
 
Delta tables are written using the `deltalake` Python library (delta-rs), a Rust implementation requiring no JVM or Spark cluster. Tables support:
 
- ACID transactions
- Schema evolution (`schema_mode="merge"`)
- Time travel (read any historical version by number)
- VACUUM and OPTIMIZE operations
 
---
 
## NOAA Ingestion — Skip Logic
 
On the first run, `fetch-noaa-csv.py` fetches data year by year from 1924 to the current year. On subsequent runs, each year is checked against MinIO before making an API call — if the file already exists in the `landing-zone` bucket it is skipped automatically. This prevents redundant API calls and avoids hitting NOAA rate limits.
 
If a year was saved locally but never uploaded to MinIO (e.g. a previous run crashed mid-way), it will be correctly re-fetched and uploaded.
 
---
 
## Stop Docker Services
 
```bash
docker compose down
```
 
Full reset (removes all volumes and stored data):
 
```bash
docker compose down -v
```
 
---
 
## Notes
 
- **Never commit** `.env` — it contains sensitive API keys
- **Never commit** `landing-zone/` — data files can be very large
- Airflow runs Python 3.11 (`apache/airflow:2.8.1-python3.11`). Dependencies are installed via `_PIP_ADDITIONAL_REQUIREMENTS` in `docker-compose.yml`
- Delta tables are stored in the MinIO `delta` bucket, not in the local `landing-zone/` folder
- The `delta` bucket is created automatically on first run by `ensure_bucket("delta")` in `delta_utils.py`