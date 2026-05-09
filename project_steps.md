# Steps
These are the steps to make the full infrastructure work.

1. `docker compose up -d` (just if the containers are stopped or missing, otherwise just skip this step)
2. Check with `docker ps`
3. Create the kafka topic
    ```bash
    docker exec -it climate-lakehouse-kafka-1 kafka-topics --create --topic weather-stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
    ```
4. Open `http://localhost:8081` (username = password = `admin`) → `climate_pipeline` → toggle `ON` → click `Trigger DAG`. Watch it in the Grid view. Each task turns green when it succeeds, red on failure.
5. Verify each zone worked
    1. Trusted zone -. ClickHouse
        ```bash
        docker exec -it clickhouse clickhouse-client --query "SELECT count() FROM trusted.noaa_bcn"
        docker exec -it clickhouse clickhouse-client --query "SELECT * FROM trusted.noaa_bcn LIMIT 5"
        ```
    2. Trusted zone -- MongoDB
        ```bash
        docker exec -it mongodb mongosh --eval "db.getSiblingDB('trusted').weather_stream.countDocuments()"
        ```
    3. Exploitation zone -- ClickHouse
        ```bash
        docker exec -it clickhouse clickhouse-client --query "SELECT count() FROM exploitation.temperature_unified"
        docker exec -it clickhouse clickhouse-client --query "SELECT * FROM exploitation.temperature_kpis LIMIT 10"
        ```
    4. Exploitation zone -- MinIO (unstructured files copied): Open `http://localhost:9001` and check that `exploitation-zone` bucket has files under `unstructured/eltiempo/` and `unstructured/satellite/`.
    5. Exploitation zone -- Milvus (embeddings)
        ```bash
        docker exec -it airflow-webserver python -c "
        from pymilvus import connections, utility
        connections.connect(host='milvus', port='19530')
        print(utility.list_collections())
        ```
    
    If a task fails, click the red task in Grid view → **More details** → **Logs** to see the full error. The most common issues on first run are:
    - Milvus not ready yet (it takes ~90s -- the script retries 10 times).
    - `delta` bucket doesn't exist yet (created by the ingestion step, so trusted zone must run after)
    - API key missing in `.env`




# Possible problems
If you get a message saying that a certain container already exists with the same name, type
```bash
docker compose down
docker compose up -d
```
If it still fails, remove it (assuming it is named minio)
```bash
docker rm -f minio
docker compose up -d
```

