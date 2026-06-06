FROM apache/airflow:2.8.1-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgomp1 \
        default-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

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
    Pillow \
    "great-expectations>=0.18,<1.0" \
    "pyspark==3.5.3" \
    "delta-spark==3.2.1"

# Pre-download the FastEmbed model at build time so it is always available at runtime
RUN python -c "from fastembed import TextEmbedding; list(TextEmbedding('BAAI/bge-small-en-v1.5').embed(['warmup']))"
