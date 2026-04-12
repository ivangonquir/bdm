from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "Poland&Spain",
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

    # Ingestion
    
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

    # Delta conversion tasks

    task_convert_delta = BashOperator(
        task_id="convert_noaa_to_delta",
        bash_command=f"{PYTHON} /opt/airflow/ingestion/convert-to-delta.py",
    )

    task_consume_kafka = BashOperator(
        task_id="consume_weather_kafka",
        bash_command=f"{PYTHON} /opt/airflow/ingestion/consume-weather-kafka.py",
    )

    task_noaa        >> task_convert_delta
    task_openweather >> task_consume_kafka

    # Declare eltiempo and satellite so Airflow tracks them properly and they appear in the DAG graph.
    [task_eltiempo, task_satellite]