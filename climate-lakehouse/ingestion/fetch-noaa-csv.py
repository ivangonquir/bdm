import requests
import os
import csv
import sys
import time
from datetime import datetime
from dotenv import load_dotenv                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)
from minio_client import upload_to_bronze


load_dotenv()
# ---------- CONFIG ----------
TOKEN = os.getenv("NOAA_TOKEN")
print(TOKEN)
DATASET_ID = "GHCND"
STATION_ID = "GHCND:SP000008181"  # Barcelona Airport
START_YEAR = 2000
END_YEAR = datetime.now().year  


url = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
headers = {"token": TOKEN}

print(f"Starting NOAA batch ingestion to CSV from {START_YEAR} to {END_YEAR}...")

for year in range(START_YEAR, END_YEAR + 1):
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    
    
    params = {
        "datasetid": DATASET_ID,
        "stationid": STATION_ID,
        "startdate": start_date,
        "enddate": end_date,
        "units": "metric",
        "limit": 1000, 
        "datatypeid": "TMIN,TMAX,TAVG",
    }
    
    print(f"Fetching data for {year}...")
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        data = response.json()
        
        if "results" in data:
            upload_to_bronze(
                source_name="NOAA",
                data_type="structured",
                format_extension="csv",
                data_content=response.text  # .text is used for strings/text
            )
            print(f"  -> Success: Saved {len(data['results'])} records of NOAA")
        else:
            print(f"  -> No data found for {year}.")
    else:
        print(f"  -> API Error {response.status_code}: {response.text}")
        
    time.sleep(1)

print("Batch CSV ingestion complete!")