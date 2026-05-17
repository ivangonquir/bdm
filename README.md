# Climate Lakehouse — BDM Project (P1 + P2)

A full Big Data Architecture for climate data, built across two project phases. The pipeline covers ingestion, landing, trusted, and exploitation zones, orchestrated with Airflow and containerized with Docker.

---

### P1 — Landing Zone

Full ingestion pipeline that fetches data from four sources and stores it in MinIO and Delta Lake:

| Script | Description |
|---|---|
| `ingestion/fetch-noaa-csv.py` | Fetches NOAA historical temperature CSVs (1924–present), skips years already in MinIO |
| `ingestion/fetch-openweather.py` | Calls OpenWeatherMap API, produces one JSON message to Kafka |
| `ingestion/fetch-eltiempo.py` | Scrapes ElTiempo HTML forecast pages, uploads to MinIO |
| `ingestion/fetch-satellite.py` | Downloads satellite temperature tile PNGs, uploads to MinIO |
| `ingestion/convert-to-delta.py` | Reads NOAA CSVs from MinIO with DuckDB, writes to Delta Lake |
| `ingestion/consume-weather-kafka.py` | Consumes Kafka topic, appends records to Delta Lake |
| `ingestion/delta_utils.py` | Shared helpers for MinIO (boto3) and Delta Lake (delta-rs) |

**Infrastructure (docker-compose.yml):**

| Service | Purpose | Port |
|---|---|---|
| Zookeeper | Kafka coordination | — |
| Kafka | Weather event streaming | 9092 |
| MinIO | Object storage (landing zone) | 9000, 9001 |
| Postgres | Airflow metadata DB | — |
| airflow-init | DB migration + admin user creation | — |
| airflow-webserver | DAG management UI | 8081 |
| airflow-scheduler | DAG execution engine | — |

---

### P2 — Trusted Zone

Cleaning scripts that read from the Delta Lake landing zone and write validated data to specialized databases.

| Script | Input | Cleaning Applied | Output |
|---|---|---|---|
| `trusted-zone/clean_noaa.py` | Delta `noaa_bcn` | Drop nulls, convert tenths→°C, validate range [-90,60]°C, deduplicate on (date, datatype, station) | ClickHouse `trusted.noaa_bcn` |
| `trusted-zone/clean_openweather.py` | Delta `weather_stream` | Deduplicate on `event_ts`, validate temp [-50,60]°C and humidity [0,100]%, fill missing strings | MongoDB `trusted.weather_stream` |
| `trusted-zone/clean_eltiempo.py` | MinIO `landing-zone/unstructured/eltiempo/` | Check min size (100 bytes), validate `<html>` tag, re-encode as UTF-8 | MinIO `trusted-zone/unstructured/eltiempo/` |
| `trusted-zone/clean_satellite.py` | MinIO `landing-zone/unstructured/satellite/` | Validate PNG magic bytes, check min size (1 KB) | MinIO `trusted-zone/unstructured/satellite/` |

**New services added (docker-compose.yml):**

| Service | Purpose | Port |
|---|---|---|
| ClickHouse | Columnar OLAP database for structured data | 8123 |
| MongoDB | Document store for semi-structured data | 27017 |
| etcd | Required by Milvus | 2379 |
| Milvus | Vector database for embeddings | 19530 |

---

### P2 — Exploitation Zone

Curation scripts that read from the Trusted Zone, join/enrich data, and produce analytics-ready assets.

| Script | Input | What It Does | Output |
|---|---|---|---|
| `exploitation-zone/build_temperature_unified.py` | ClickHouse `trusted.noaa_bcn` + MongoDB `trusted.weather_stream` | Joins NOAA + OpenWeather into one denormalised table | ClickHouse `exploitation.temperature_unified` |
| `exploitation-zone/compute_kpis.py` | ClickHouse `exploitation.temperature_unified` | Pre-computes monthly and seasonal avg/min/max KPIs | ClickHouse `exploitation.temperature_kpis` |
| `exploitation-zone/curate_weather.py` | MongoDB `trusted.weather_stream` | Derives `season`, `comfort_index`, `is_extreme` fields | MongoDB `exploitation.weather_curated` |
| `exploitation-zone/organize_unstructured.py` | MinIO `trusted-zone/unstructured/` | Server-side copy to exploitation bucket | MinIO `exploitation-zone/unstructured/` |
| `exploitation-zone/compute_embeddings.py` | MinIO `exploitation-zone/unstructured/eltiempo/` | Extracts HTML text, generates 384-dim embeddings (FastEmbed / BAAI/bge-small-en-v1.5) | Milvus `eltiempo_embeddings` |

---

### Orchestration (DAG)

The Airflow DAG `climate_pipeline` covers the full pipeline end-to-end:

```
fetch_noaa ──► convert_noaa_to_delta ──► clean_noaa ──┐
                                                        ├──► build_temperature_unified ──► compute_kpis
fetch_openweather ──► consume_weather_kafka ──► clean_openweather ──┘
                                                    └──► curate_weather

fetch_eltiempo ──► clean_eltiempo ──┐
                                    ├──► organize_unstructured ──► compute_embeddings
fetch_satellite ──► clean_satellite ─┘
```

---

## What Still Needs to Be Done

### 1. Data Consumption (Required — Constraint 5)

Nothing consumes the data in the Exploitation Zone yet. At least one downstream task must be implemented. The natural candidates given the existing data are:

- **Streamlit dashboard** — visualise the temperature KPIs from `exploitation.temperature_kpis` (avg/min/max per month and season, historical trends from 1924 to present)
- **RAG chatbot** — use the Milvus embeddings of ElTiempo forecasts to answer natural-language weather queries
- **Alert system** — trigger alerts when `exploitation.weather_curated` contains `is_extreme = True` readings

The requirement is that something reads from the Exploitation Zone and produces a result. A minimal but functional implementation is sufficient.

### 2. Architecture Diagram (Required — Constraint 6)

An updated architecture diagram showing all zones, tools, data flows, and new services (ClickHouse, MongoDB, Milvus) must be produced for the report.

### 3. Data Governance (Optional — contributes to grade)

At least one governance mechanism implemented over the architecture:

- **Data quality validation** — e.g. Great Expectations checks after each cleaning step
- **Lineage tracking** — trace how data moves from NOAA → Delta → ClickHouse → KPIs
- **Data catalog** — describe data products in the Exploitation Zone using DCAT
- **Access control** — define roles per zone (e.g. raw data restricted to engineers, curated data open to analysts)

### 4. Known Bugs to Fix (before final submission)

| File | Issue |
|---|---|
| `trusted-zone/clean_openweather.py:68` | Numpy scalar conversion creates a new dict but never writes it back to `records` — the conversion is a no-op |
| `trusted-zone/clean_eltiempo.py:28` | `list_objects_v2` returns max 1000 objects with no pagination — files beyond 1000 are silently dropped |
| `trusted-zone/clean_satellite.py:28` | Same pagination issue |
| `exploitation-zone/organize_unstructured.py:28` | Same pagination issue |

---

## Folder Structure

```
climate-lakehouse/
│
├── airflow/
│   ├── Dockerfile                        # Pre-installs all Python deps (avoids runtime pip)
│   └── dags/
│       └── ingestion-dag.py             # Full end-to-end Airflow DAG
│
├── ingestion/                            # Landing zone scripts
│   ├── delta_utils.py
│   ├── fetch-noaa-csv.py
│   ├── fetch-openweather.py
│   ├── fetch-eltiempo.py
│   ├── fetch-satellite.py
│   ├── convert-to-delta.py
│   └── consume-weather-kafka.py
│
├── trusted-zone/                         # Cleaning scripts (Landing → Trusted)
│   ├── clean_noaa.py
│   ├── clean_openweather.py
│   ├── clean_eltiempo.py
│   └── clean_satellite.py
│
├── exploitation-zone/                    # Curation scripts (Trusted → Exploitation)
│   ├── build_temperature_unified.py
│   ├── compute_kpis.py
│   ├── curate_weather.py
│   ├── organize_unstructured.py
│   └── compute_embeddings.py
│
├── landing-zone/                         # Local staging (not committed)
├── .env                                  # API keys — never commit
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Setup Instructions

### 1. Configure API Keys

Create a `.env` file in the project root:

```env
OPENWEATHER_KEY=your_openweather_api_key
NOAA_TOKEN=your_noaa_api_token
AIRFLOW__WEBSERVER__SECRET_KEY=any_random_string
```

> `AIRFLOW__WEBSERVER__SECRET_KEY` must be set and identical across all Airflow containers. Without it each container generates a different key, causing 403 errors in the task log UI.

### 2. Start all services

```bash
docker compose up -d
```

Allow ~2 minutes on first boot for `airflow-init` to finish.

### 3. Create the Kafka topic

```bash
docker exec -it climate-kafka-1 kafka-topics --create \
  --topic weather-stream \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

### 4. Run the pipeline

1. Open Airflow at http://localhost:8081 (admin / admin)
2. Go to **DAGs** → `climate_pipeline`
3. Toggle the DAG **ON**, then click **Trigger DAG**

Expected run time: ~5–8 minutes on first run. Subsequent runs are faster (NOAA skips years already in MinIO).

### 5. Access service UIs

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8081 | admin / admin |
| MinIO | http://localhost:9001 | minioadmin / minioadmin |
| ClickHouse | http://localhost:8123 | no auth |
| MongoDB | localhost:27017 | no auth |
| Milvus | localhost:19530 | no auth |

### 6. Stop services

```bash
docker compose down
```

Full reset (removes all stored data):

```bash
docker compose down -v
```

---

## Technology Stack

| Concern | Technology |
|---|---|
| Object storage | MinIO (S3-compatible) |
| Open Table Format | Delta Lake via delta-rs (no JVM) |
| Query engine (ingestion) | DuckDB |
| Streaming | Apache Kafka |
| Structured store | ClickHouse (columnar OLAP) |
| Document store | MongoDB |
| Vector store | Milvus |
| Embeddings | FastEmbed (BAAI/bge-small-en-v1.5, ONNX, CPU) |
| Orchestration | Apache Airflow 2.8.1 |
| Containerization | Docker Compose |

---

## Data Sources

| Source | Type | Description |
|---|---|---|
| NOAA (CDO API) | Structured | Daily temperature records for Barcelona (GHCND:SP000008181), 1924–present |
| OpenWeatherMap API | Semi-structured | Current weather readings, one per DAG run, streamed via Kafka |
| ElTiempo | Unstructured | Scraped HTML forecast pages |
| Satellite tiles | Unstructured | Temperature map PNG tiles |
