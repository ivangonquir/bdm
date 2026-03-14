FOLDER STRUCTURE

climate-lakehouse/
│
├─ landing-zone/
│   ├─ structured/       
│   ├─ semi-structured/   
│   └─ unstructured/      
│
├─ ingestion/
│   └─ fetch-openweather.py
│   └─ fetch-eltiempo.py
│   └─ fetch-noaa.py
│   └─ fetch-satellite.py   
│
├─ docker-compose.yml
└─ README.txt

----------------------------------------------------------------------
REQUIREMENTS

- Python 3.10+
- pip packages:

    pip install -r requirements.txt

- Docker Desktop (to run Kafka and Zookeeper)
- OpenWeatherMap API key 

----------------------------------------------------------------------
SETUP INSTRUCTIONS

1. Docker – Kafka & Zookeeper

Make sure Docker Desktop is running. Then open a terminal in the project root folder (climate-lakehouse) and run:

    docker compose up -d

This will start:

- Zookeeper (manages Kafka state)
- Kafka broker (listens on localhost:9092)

Check running containers:

    docker ps

You should see both Kafka and Zookeeper running.

----------------------------------------------------------------------
2. Create Kafka Topic

Create a topic to receive weather data:

    docker exec -it project1-kafka-1 kafka-topics --create --topic weather-stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

Verify it exists:

    docker exec -it project1-kafka-1 kafka-topics --list --bootstrap-server localhost:9092

You should see:

    weather-stream

----------------------------------------------------------------------
3. Configure OpenWeatherMap

Create a `.env` file in the project root:

    OPENWEATHER_KEY=YOUR_API_KEY_HERE

Replace YOUR_API_KEY_HERE with your OpenWeatherMap API key.

----------------------------------------------------------------------
4. Run Python Streaming Script

Run the OpenWeatherMap producer:

    python ingestion/fetch-openweather.py

- JSON files will be saved in:
    landing-zone/semi-structured/openweathermap/
- Data is also sent to Kafka topic 'weather-stream' (Hot Path ingestion).

----------------------------------------------------------------------
6. Stop Kafka / Docker

When finished:

    docker compose down

This stops both Kafka and Zookeeper containers.

----------------------------------------------------------------------

- Make sure Docker Desktop is running before starting Python script.
- Ensure your .env file has a valid OpenWeatherMap API key.