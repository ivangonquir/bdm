CLIMATE LAKEHOUSE PROJECT
README.txt

----------------------------------------------------------------------

FOLDER STRUCTURE

climate-lakehouse/
│
├─ landing-zone/
│   ├─ structured/          (NOAA CSV datasets)
│   ├─ semi-structured/     (OpenWeather JSON data)
│   └─ unstructured/        (HTML pages and satellite images)
│
├─ ingestion/
│   ├─ fetch-openweather.py
│   ├─ fetch-eltiempo.py
│   ├─ fetch-noaa.py
│   └─ fetch-satellite.py
│
├─ docker-compose.yml
└─ README.txt

----------------------------------------------------------------------

PROJECT DESCRIPTION

This project implements a simple Climate Data Lakehouse ingestion layer.

It collects data from multiple sources and stores them in an object
storage landing zone.

Data sources used:

- NOAA Climate Data (structured CSV)
- OpenWeatherMap API (semi-structured JSON)
- ElTiempo website scraping (unstructured HTML)
- OpenWeather satellite temperature tiles (unstructured images)

Streaming ingestion is implemented using Kafka.

Object storage is implemented using MinIO.

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

Docker Compose will start the following services:

1. Zookeeper
   Manages Kafka metadata and cluster coordination.

2. Kafka
   Receives streaming weather data.

3. MinIO
   S3-compatible object storage used as the Data Lake landing zone.

----------------------------------------------------------------------

SETUP INSTRUCTIONS

1. Start Docker Services

Make sure Docker Desktop is running.

Open a terminal in the project root folder (climate-lakehouse) and run:

    docker compose up -d

This will start:

- Zookeeper
- Kafka
- MinIO object storage

Check running containers:

    docker ps

----------------------------------------------------------------------

2. Access MinIO Object Storage

Open your browser and go to:

    http://localhost:9001

Login credentials:

    Username: minioadmin
    Password: minioadmin

Create a bucket called:

    landing-zone

This bucket will store all ingested data.

----------------------------------------------------------------------

3. Create Kafka Topic

Create a topic to receive weather streaming data:

    docker exec -it project1-kafka-1 kafka-topics --create --topic weather-stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

Verify the topic exists:

    docker exec -it project1-kafka-1 kafka-topics --list --bootstrap-server localhost:9092

You should see:

    weather-stream

----------------------------------------------------------------------

4. Configure API Keys

Create a `.env` file in the project root directory:

    OPENWEATHER_KEY=YOUR_OPENWEATHER_API_KEY
    NOAA_TOKEN=YOUR_NOAA_API_TOKEN

Replace the placeholders with your actual API keys.

----------------------------------------------------------------------

5. Run Data Ingestion Scripts

All scripts are located inside the ingestion folder.

Run them from the project root.

Example:

    python ingestion/fetch-openweather.py
    python ingestion/fetch-noaa.py
    python ingestion/fetch-eltiempo.py
    python ingestion/fetch-satellite.py

Each script performs the following steps:

1. Fetch data from the source
2. Save a local copy inside the landing-zone folder
3. Upload the file to MinIO object storage

----------------------------------------------------------------------

6. Data Storage Structure

Inside MinIO, data will be stored in the following structure:

landing-zone/
│
├─ structured/
│   └─ noaa/
│       └─ noaa_bcn_YEAR.csv
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

----------------------------------------------------------------------

7. Streaming Data (Kafka)

The OpenWeatherMap script also sends weather data to Kafka.

Kafka topic used:

    weather-stream

This enables real-time ingestion for future streaming processing.

----------------------------------------------------------------------

8. Stop Docker Services

When finished working with the project:

    docker compose down

This stops Kafka, Zookeeper, and MinIO containers.

Your local files and object storage data remain saved.

----------------------------------------------------------------------

NOTES

- Make sure Docker Desktop is running before executing Python scripts.
- Ensure the `.env` file contains valid API keys.
- MinIO must be running for object storage uploads to work.
- The landing-zone folder acts as the local staging area for ingested data.

----------------------------------------------------------------------