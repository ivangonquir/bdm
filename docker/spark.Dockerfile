FROM spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

# Install Python 3.11 and make it the default python3 so Spark picks it up
# regardless of which code path resolves the Python binary.
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3.11 \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3.11 /usr/bin/python3

ENV PYSPARK_PYTHON=python3.11

USER spark
