# Climate Lakehouse Project

This project implements a multi-modal Data Lakehouse architecture to ingest, process, and store climate data from various sources using Python, Kafka, and MinIO.

## 📁 FOLDER STRUCTURE
```text
climate-lakehouse/
│
├─ ingestion/
│  ├─ fetch-openweather.py      # Producer: Grabs live weather API and sends to Kafka
│  ├─ consume-openweather.py    # Consumer: Listens to Kafka and uploads to MinIO
│  ├─ fetch-eltiempo.py         # Batch: Regional weather directly to MinIO
│  ├─ fetch-noaa.py             # Batch: Historical climate data directly to MinIO
│  └─ fetch-satellite.py        # Batch: Raster/Image maps directly to MinIO
│
├─ docker-compose.yml           # Infrastructure (Kafka, Zookeeper, MinIO)
├─ minio_client.py              # Helper functions to upload data to the Lakehouse
├─ requirements.txt             # Python dependencies
└─ README.md    
```              


## ⚙️ REQUIREMENTS
- Python 3.10+

- Docker Desktop (to run Kafka, Zookeeper, and MinIO)

- API Keys required:
    - OpenWeatherMap API key (for live data and satellite maps)
    - NOAA CDO Token (for historical data)
    
- Important Python packages: Install them by running

    ```Bash
    pip install -r requirements.txt
    ```

## 🚀 SETUP INSTRUCTIONS
1. Start the Infrastructure (Docker)
Make sure Docker Desktop is running. Open a terminal in the project root folder and run:

    ```Bash
    docker compose up -d
    ```
    This will start:

    - Zookeeper (manages Kafka state)

    - Kafka broker (Message queue listening on port 9092)

    - MinIO (Our S3-compatible Data Lakehouse storage)

    Verify MinIO: Open your browser and go to `http://localhost:9001` (User: `minioadmin`, Pass: `minioadmin`).

2. Create the Kafka Topic
We need to create a "mailbox" for the streaming data. Run this command to create the topic (Note: replace kafka-1 with your actual container name if it differs when you run `docker ps`):

    ```Bash
    docker exec -it kafka-1 kafka-topics --create --topic weather-stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
    ```
3. Configure Environment Variables
Create a `.env` file in the project root to store your API keys safely:

    ```Plaintext
    OPENWEATHER_KEY=your_openweather_api_key_here
    NOAA_TOKEN=your_noaa_token_here
    ```

## 🏃‍♂️ RUNNING THE INGESTION PIPELINES
We have two different paths to load and save the data.

**Option A**: The Streaming Pipeline (OpenWeather)
Because this is an event-driven stream, you need two terminal windows.

- Terminal 1 (Start the Consumer):

    ```Bash
    python ingestion/consume-openweather.py
    ```
    (It will stay open, actively listening for new messages).

- Terminal 2 (Run the Producer):

    ```Bash
    python ingestion/fetch-openweather.py
    ```
    (The data will be sent to Kafka, caught by the Consumer, and uploaded directly to MinIO).

**Option B**: The Batch Pipelines (Direct to MinIO)
These scripts bypass Kafka and upload historical or large unstructured data directly into the Lakehouse. Run any of these in your terminal:

```Bash
python ingestion/fetch-eltiempo.py
python ingestion/fetch-noaa.py
python ingestion/fetch-satellite.py
```

## 🛑 SHUTDOWN AND CLEANUP
When finished, stop the infrastructure:

```Bash
docker compose down
```