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
    dag_id="climate_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # ── LANDING ZONE: ingestion ───────────────────────────────────────────────

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
    task_convert_delta = BashOperator(
        task_id="convert_noaa_to_delta",
        bash_command=f"{PYTHON} /opt/airflow/ingestion/convert-to-delta.py",
    )
    task_consume_kafka = BashOperator(
        task_id="consume_weather_kafka",
        bash_command=f"{PYTHON} /opt/airflow/ingestion/consume-weather-kafka.py",
    )

    # ── TRUSTED ZONE: cleaning ────────────────────────────────────────────────

    task_clean_noaa = BashOperator(
        task_id="clean_noaa",
        bash_command=f"{PYTHON} /opt/airflow/trusted-zone/clean_noaa.py",
    )
    task_clean_openweather = BashOperator(
        task_id="clean_openweather",
        bash_command=f"{PYTHON} /opt/airflow/trusted-zone/clean_openweather.py",
    )
    task_clean_eltiempo = BashOperator(
        task_id="clean_eltiempo",
        bash_command=f"{PYTHON} /opt/airflow/trusted-zone/clean_eltiempo.py",
    )
    task_clean_satellite = BashOperator(
        task_id="clean_satellite",
        bash_command=f"{PYTHON} /opt/airflow/trusted-zone/clean_satellite.py",
    )

    # ── EXPLOITATION ZONE: curation & enrichment ──────────────────────────────

    task_build_unified = BashOperator(
        task_id="build_temperature_unified",
        bash_command=f"{PYTHON} /opt/airflow/exploitation-zone/build_temperature_unified.py",
    )
    task_compute_kpis = BashOperator(
        task_id="compute_kpis",
        bash_command=f"{PYTHON} /opt/airflow/exploitation-zone/compute_kpis.py",
    )
    task_curate_weather = BashOperator(
        task_id="curate_weather",
        bash_command=f"{PYTHON} /opt/airflow/exploitation-zone/curate_weather.py",
    )
    task_organize_unstruct = BashOperator(
        task_id="organize_unstructured",
        bash_command=f"{PYTHON} /opt/airflow/exploitation-zone/organize_unstructured.py",
    )
    task_compute_embeddings = BashOperator(
        task_id="compute_embeddings",
        bash_command=f"{PYTHON} /opt/airflow/exploitation-zone/compute_embeddings.py",
    )

    # ── DEPENDENCIES ──────────────────────────────────────────────────────────

    # Landing → Trusted (structured)
    task_noaa >> task_convert_delta >> task_clean_noaa

    # Landing → Trusted (semi-structured)
    task_openweather >> task_consume_kafka >> task_clean_openweather

    # Landing → Trusted (unstructured)
    task_eltiempo >> task_clean_eltiempo
    task_satellite >> task_clean_satellite

    # Trusted → Exploitation (structured: join + KPIs)
    [task_clean_noaa, task_clean_openweather] >> task_build_unified >> task_compute_kpis

    # Trusted → Exploitation (semi-structured: enrichment)
    task_clean_openweather >> task_curate_weather

    # Trusted → Exploitation (unstructured: copy + embeddings)
    [task_clean_eltiempo, task_clean_satellite] >> task_organize_unstruct >> task_compute_embeddings
