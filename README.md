# Climate Lakehouse — BDM Project (P1 + P2)

A full Big Data Architecture for climate data, built across two project phases. The pipeline covers ingestion, trusted zone cleaning, exploitation zone curation, data governance, and interactive data consumption — orchestrated with Airflow and containerized with Docker.

---

## Project Status

| Constraint | Deliverable | Status |
|---|---|---|
| Landing Zone | Ingestion pipeline (NOAA, OpenWeather, ElTiempo, Satellite) | Done |
| Trusted Zone | Cleaning + validation into ClickHouse / MongoDB / MinIO | Done |
| Exploitation Zone | Unified table, KPIs, curated docs, embeddings | Done |
| Data Consumption | Streamlit dashboard + RAG chatbot | Done |
| Data Governance | Great Expectations quality validation | Done |
| Architecture Diagram | For the report | Pending |

---

## P1 — Landing Zone

Ingestion scripts that fetch raw data from four sources and store it in MinIO and Delta Lake.

| Script | Description |
|---|---|
| `ingestion/fetch-noaa-csv.py` | Fetches NOAA historical temperature CSVs (1924–present), skips years already in MinIO |
| `ingestion/fetch-openweather.py` | Calls OpenWeatherMap API, produces one JSON message to Kafka |
| `ingestion/fetch-eltiempo.py` | Scrapes ElTiempo HTML forecast pages, uploads to MinIO |
| `ingestion/fetch-satellite.py` | Downloads satellite temperature tile PNGs, uploads to MinIO |
| `ingestion/convert-to-delta.py` | Reads NOAA CSVs from MinIO with DuckDB, writes to Delta Lake |
| `ingestion/consume-weather-kafka.py` | Consumes Kafka topic, appends records to Delta Lake |
| `ingestion/delta_utils.py` | Shared helpers for MinIO (boto3) and Delta Lake (delta-rs) |

---

## P2 — Trusted Zone

Cleaning scripts that read from the landing zone and write validated data to specialised databases.

| Script | Input | Cleaning Applied | Output |
|---|---|---|---|
| `trusted-zone/clean_noaa.py` | Delta `noaa_bcn` | Drop nulls, convert tenths→°C, validate range [−90, 60] °C, deduplicate | ClickHouse `trusted.noaa_bcn` |
| `trusted-zone/clean_openweather.py` | Delta `weather_stream` | Deduplicate on `event_ts`, validate temp and humidity ranges | MongoDB `trusted.weather_stream` |
| `trusted-zone/clean_eltiempo.py` | MinIO `landing-zone/unstructured/eltiempo/` | Validate HTML, re-encode UTF-8 | MinIO `trusted-zone/unstructured/eltiempo/` |
| `trusted-zone/clean_satellite.py` | MinIO `landing-zone/unstructured/satellite/` | Validate PNG magic bytes, check min size | MinIO `trusted-zone/unstructured/satellite/` |

---

## P2 — Exploitation Zone

Curation scripts that read from the Trusted Zone and produce analytics-ready assets.

| Script | Input | What It Does | Output |
|---|---|---|---|
| `exploitation-zone/build_temperature_unified.py` | ClickHouse `trusted.noaa_bcn` + MongoDB `trusted.weather_stream` | Joins NOAA + OpenWeather into one denormalised table | ClickHouse `exploitation.temperature_unified` |
| `exploitation-zone/compute_kpis.py` | ClickHouse `exploitation.temperature_unified` | Pre-computes monthly and seasonal avg/min/max KPIs | ClickHouse `exploitation.temperature_kpis` |
| `exploitation-zone/curate_weather.py` | MongoDB `trusted.weather_stream` | Derives `season`, `comfort_index`, `is_extreme` fields | MongoDB `exploitation.weather_curated` |
| `exploitation-zone/organize_unstructured.py` | MinIO `trusted-zone/unstructured/` | Server-side copy to exploitation bucket | MinIO `exploitation-zone/unstructured/` |
| `exploitation-zone/compute_embeddings.py` | MinIO `exploitation-zone/unstructured/eltiempo/` | Extracts HTML text, generates 384-dim embeddings (FastEmbed) | Milvus `eltiempo_embeddings` |

---

## P2 — Data Governance

Quality validation runs after all cleaning steps and before exploitation. Results are persisted in MongoDB for audit trails.

| Script | What It Validates | On Failure |
|---|---|---|
| `governance/validate_quality.py` | NOAA: nulls, temperature range, valid datatypes · OpenWeather: nulls, temp/humidity ranges, unique timestamps | Exits with code 1 — Airflow marks the task failed and blocks downstream exploitation tasks |

Validation reports are stored in MongoDB `governance.quality_results` with per-expectation detail (expectation type, column, pass/fail, result values).

---

## P2 — Data Consumption

A Streamlit dashboard at [http://localhost:8501](http://localhost:8501) with two pages:

**Page 1 — Climate Dashboard**
Reads from ClickHouse `exploitation.temperature_kpis` and displays:
- Seasonal KPI cards (avg, min, max per season)
- Annual temperature trend with min/max band (1924–present)
- Average monthly profile bar chart
- Seasonal averages bar chart
- Month × Year temperature heatmap
- Year range slider to filter all charts

**Page 2 — Climate Q&A (RAG)**
Semantic search over ElTiempo forecast pages powered by Milvus:
- User types a natural-language weather question
- FastEmbed embeds the query (same model used at ingestion time)
- Milvus returns the top-3 most similar ElTiempo passages
- Each passage is shown with its filename and similarity score
- If `GROQ_API_KEY` is set, Groq (`llama3-8b-8192`) generates a prose answer grounded in those passages

---

## Orchestration (DAG)

The Airflow DAG `climate_pipeline` covers the full pipeline end-to-end:

```
fetch_noaa ──► convert_noaa_to_delta ──► clean_noaa ──┐
                                                        ├──► validate_quality ──► build_temperature_unified ──► compute_kpis
fetch_openweather ──► consume_weather_kafka ──► clean_openweather ──┘
                                                    └──► curate_weather

fetch_eltiempo ──► clean_eltiempo ──┐
                                    ├──► organize_unstructured ──► compute_embeddings
fetch_satellite ──► clean_satellite ─┘
```

---

## Infrastructure

| Service | Purpose | Port |
|---|---|---|
| Zookeeper | Kafka coordination | — |
| Kafka | Weather event streaming | 9092 |
| MinIO | Object storage (all zones) | 9000 (API), 9001 (UI) |
| ClickHouse | Columnar OLAP store (structured data) | 8123 |
| MongoDB | Document store (semi-structured + governance) | 27017 |
| spark-master | Spark cluster master | 7077, 8080 (UI) |
| spark-worker | Spark executor node | — |
| etcd | Required by Milvus | — |
| Milvus | Vector store (embeddings) | 19530 |
| Streamlit | Dashboard + RAG chatbot | 8501 |
| Postgres | Airflow metadata DB | — |
| airflow-init | DB migration + admin user creation | — |
| airflow-webserver | DAG management UI | 8081 |
| airflow-scheduler | DAG execution engine | — |

---

## Setup Instructions

### 1. Configure API keys

Create a `.env` file in the project root:

```env
OPENWEATHER_KEY=your_openweather_api_key
NOAA_TOKEN=your_noaa_api_token
AIRFLOW__WEBSERVER__SECRET_KEY=any_random_string

# Optional — enables AI-generated answers in the Climate Q&A page
GROQ_API_KEY=your_groq_api_key
```

> `AIRFLOW__WEBSERVER__SECRET_KEY` must be set and identical across all Airflow containers. Without it each container generates a different key, causing 403 errors in the task log UI.

> A free Groq API key can be obtained at [console.groq.com](https://console.groq.com). Without it the Q&A page works in retrieval-only mode.

### 2. Build and start all services

On first run, or after any Dockerfile change:

```bash
docker compose build
docker compose up -d
```

Allow ~2 minutes on first boot for `airflow-init` to finish and Milvus to become healthy.

To rebuild only one service (e.g. after changing the dashboard):

```bash
docker compose build streamlit
docker compose up -d streamlit
```

> **Troubleshooting — Python version mismatch on Spark workers**
> If Airflow tasks that use Spark fail with `[PYTHON_VERSION_MISMATCH] Python in worker has different version (3, 10) than that in driver 3.11`, rebuild the Spark images without cache to ensure Python 3.11 is properly installed:
> ```bash
> docker compose build --no-cache spark-master spark-worker
> docker compose up -d
> ```

### 3. Create the Kafka topic

```bash
docker compose exec kafka kafka-topics --create \
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
| Spark UI | http://localhost:8080 | — |
| MinIO | http://localhost:9001 | minioadmin / minioadmin |
| Streamlit dashboard | http://localhost:8501 | — |
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

## Folder Structure

```
climate-lakehouse/
│
├── airflow/
│   ├── Dockerfile                       # Installs all Python deps + Java + PySpark + pre-downloads FastEmbed model
│   └── dags/
│       └── ingestion-dag.py             # Full end-to-end Airflow DAG
│
├── docker/
│   └── spark.Dockerfile                 # Custom Spark image with Python 3.11 for driver/worker version alignment
│
├── ingestion/                           # Landing zone scripts
│   ├── delta_utils.py
│   ├── fetch-noaa-csv.py
│   ├── fetch-openweather.py
│   ├── fetch-eltiempo.py
│   ├── fetch-satellite.py
│   ├── convert-to-delta.py
│   └── consume-weather-kafka.py
│
├── trusted-zone/                        # Cleaning scripts (Landing → Trusted)
│   ├── clean_noaa.py
│   ├── clean_openweather.py
│   ├── clean_eltiempo.py
│   └── clean_satellite.py
│
├── exploitation-zone/                   # Curation scripts (Trusted → Exploitation)
│   ├── build_temperature_unified.py
│   ├── compute_kpis.py
│   ├── curate_weather.py
│   ├── organize_unstructured.py
│   └── compute_embeddings.py
│
├── governance/                          # Data quality validation
│   └── validate_quality.py
│
├── consumption/                         # Streamlit dashboard + RAG chatbot
│   ├── dashboard.py
│   └── Dockerfile
│
├── landing-zone/                        # Local staging (not committed)
├── .env                                 # API keys — never commit
├── docker-compose.yml
└── README.md
```

---

## Technology Stack

| Concern | Technology |
|---|---|
| Object storage | MinIO (S3-compatible) |
| Open Table Format | Delta Lake via delta-spark 3.2.1 |
| Distributed processing | Apache Spark 3.5.3 (PySpark, standalone cluster) |
| Query engine (ingestion) | DuckDB |
| Streaming | Apache Kafka |
| Structured store | ClickHouse (columnar OLAP) |
| Document store | MongoDB |
| Vector store | Milvus |
| Embeddings | FastEmbed (BAAI/bge-small-en-v1.5, ONNX, CPU) |
| Data governance | Great Expectations 0.18 |
| Visualisation | Streamlit + Plotly |
| RAG / LLM | Milvus similarity search + Groq (llama3-8b-8192, optional) |
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
