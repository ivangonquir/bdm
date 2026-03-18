import requests
import os
import json
import sys
import time
from dotenv import load_dotenv
from datetime import datetime

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)
from minio_client import upload_to_bronze


load_dotenv()
# ---------- CONFIG ----------
TOKEN = os.getenv("NOAA_TOKEN")
DATASET_ID = "GHCND"
STATION_ID = "GHCND:SP000008181"  # Barcelona Airport
START_YEAR = 2000
END_YEAR = datetime.now().year  # 2026


url = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
headers = {"token": TOKEN}

print(f"Starting NOAA batch ingestion from {START_YEAR} to {END_YEAR}...")

# Loop through each year to respect the API's 1-year limit
for year in range(START_YEAR, END_YEAR + 1):
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    
    # Standardized naming convention based on logical properties [cite: 198, 202]

    
    params = {
        "datasetid": DATASET_ID,
        "stationid": STATION_ID,
        "startdate": start_date,
        "enddate": end_date,
        "units": "metric",
        "limit": 1000,  # 365 days easily fits under the 1000 record limit
        "datatypeid": "TMIN,TMAX,TAVG",
    }
    
    print(f"Fetching data for {year}...")
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        data = response.json()
        
        # Check if data exists for this specific year
        if "results" in data:
            upload_to_bronze(
                source_name="NOAA",
                data_type="structured",
                format_extension="json",
                data_content=response.text
            )
            print(f"  -> Success: Saved {len(data['results'])} records of NOAA json.")
        else:
            print(f"  -> No data found for {year}.")
    else:
        print(f"  -> API Error {response.status_code}: {response.text}")
        
    # Pause for 1 second between requests to avoid getting blocked by NOAA
    time.sleep(1)

print("Batch ingestion complete!")