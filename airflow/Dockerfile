FROM apache/airflow:2.8.1-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgomp1 \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    boto3 \
    kafka-python \
    python-dotenv \
    requests \
    duckdb \
    "deltalake>=0.17" \
    pyarrow \
    clickhouse-connect \
    pymongo \
    pymilvus \
    fastembed \
    beautifulsoup4 \
    Pillow
