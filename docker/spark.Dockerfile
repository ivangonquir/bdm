FROM spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

# Python 3.11 is available in Ubuntu 22.04 default repos — no PPA needed.
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3.11 \
    && rm -rf /var/lib/apt/lists/*

# Tell Spark executors to use Python 3.11, matching the driver (Airflow = 3.11).
ENV PYSPARK_PYTHON=python3.11

USER spark
