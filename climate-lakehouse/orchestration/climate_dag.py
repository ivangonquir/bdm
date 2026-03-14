from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default settings for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG (Daily Batch Pipeline)
with DAG(
    'climate_batch_ingestion',
    default_args=default_args,
    description='Automated batch ingestion for climate data',
    schedule_interval=timedelta(days=1), # Runs daily
    catchup=False
) as dag:

    # Define tasks using BashOperator to run your Python scripts
    fetch_noaa = BashOperator(
        task_id='fetch_noaa_data',
        bash_command='python /path/to/your/project/climate-lakehouse/ingestion/fetch-noaa-csv.py'
    )

    fetch_satellite = BashOperator(
        task_id='fetch_satellite_data',
        bash_command='python /path/to/your/project/climate-lakehouse/ingestion/fetch-satellite.py'
    )
    
    fetch_eltiempo = BashOperator(
        task_id='fetch_eltiempo_data',
        bash_command='python /path/to/your/project/climate-lakehouse/ingestion/fetch-eltiempo.py'
    )

    # Set dependencies (they can run in parallel, or sequentially if you prefer)
    [fetch_noaa, fetch_satellite, fetch_eltiempo]