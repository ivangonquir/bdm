from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# FIX: deps are installed at container startup in docker-compose, not here.
# Running pip install on every task execution is slow and can cause race conditions.
PYTHON = "python"

with DAG(
    dag_id="climate_ingestion_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

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

    # FIX: submit the Spark job to the Spark master cluster (not run locally inside Airflow).
    # The spark_job/ folder is mounted into the Spark containers at /opt/spark_jobs/.
    task_convert_delta = BashOperator(
        task_id="convert_to_delta",
        bash_command=(
            "docker exec spark spark-submit "
            "--packages io.delta:delta-spark_2.12:3.1.0 "
            "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
            "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
            "/opt/spark_jobs/convert-to-delta.py"
        ),
    )

    # FIX: set actual dependencies.
    # All four fetch tasks run in parallel, then delta conversion runs after NOAA (needs CSV).
    [task_openweather, task_eltiempo, task_satellite]  # these run in parallel, no deps
    task_noaa >> task_convert_delta                    # delta conversion waits for NOAA data