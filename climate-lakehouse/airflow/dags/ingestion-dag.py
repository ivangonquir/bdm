from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

PYTHON = "python"

with DAG(
    dag_id="climate_ingestion_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # ── Ingestion tasks ───────────────────────────────────────────────────────

    task_noaa = BashOperator(
        task_id="fetch_noaa",
        bash_command=f"{PYTHON} /opt/airflow/ingestion/fetch-noaa-csv.py",
    )

    task_openweather = BashOperator(
        task_id="fetch_openweather",
        bash_command=f"{PYTHON} /opt/airflow/ingestion/fetch-openweather.py",
    )

    task_eltiempo = BashOperator(
        task_id="fetch_eltiempo",
        bash_command=f"{PYTHON} /opt/airflow/ingestion/fetch-eltiempo.py",
    )

    task_satellite = BashOperator(
        task_id="fetch_satellite",
        bash_command=f"{PYTHON} /opt/airflow/ingestion/fetch-satellite.py",
    )

    # ── Delta conversion tasks ────────────────────────────────────────────────

    # Reads NOAA CSVs → writes Delta table s3://delta/noaa_bcn
    task_convert_delta = BashOperator(
        task_id="convert_noaa_to_delta",
        bash_command=f"{PYTHON} /opt/airflow/ingestion/convert-to-delta.py",
    )

    # Reads weather-stream Kafka topic → writes Delta table s3://delta/weather_stream
    task_consume_kafka = BashOperator(
        task_id="consume_weather_kafka",
        bash_command=f"{PYTHON} /opt/airflow/ingestion/consume-weather-kafka.py",
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    #
    #   fetch_noaa        ──► convert_noaa_to_delta
    #   fetch_openweather ──► consume_weather_kafka
    #   fetch_eltiempo     ─┐  (parallel, no downstream)
    #   fetch_satellite    ─┘

    task_noaa        >> task_convert_delta
    task_openweather >> task_consume_kafka