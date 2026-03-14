import requests
import os
import csv
import time
from datetime import datetime

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)
from landing_zone.minio_client import upload_to_bronze


# ---------- CONFIG ----------
TOKEN = os.getenv("NOAA_TOKEN")
DATASET_ID = "GHCND"
STATION_ID = "GHCND:SP000008181"  # Barcelona Airport
START_YEAR = 2000
END_YEAR = datetime.now().year  

# Establish the Landing Zone folder path
FOLDER = "../landing-zone/structured/noaa"
os.makedirs(FOLDER, exist_ok=True)

url = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
headers = {"token": TOKEN}

print(f"Starting NOAA batch ingestion to CSV from {START_YEAR} to {END_YEAR}...")

for year in range(START_YEAR, END_YEAR + 1):
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    
    # Save as CSV now
    filename = f"{FOLDER}/noaa_bcn_{year}.csv"
    
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
            # Open the file and write as CSV
            """
            with open(filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                # Write the header row
                writer.writerow(["date", "datatype", "station", "value", "attributes"])
                
                # Write the data rows
                for item in data["results"]:
                    writer.writerow([
                        item.get("date"), 
                        item.get("datatype"), 
                        item.get("station"), 
                        item.get("value"), 
                        item.get("attributes")
                    ])
            """

            upload_to_bronze(
                source_name="NOAA",
                data_type="structured",
                format_extension="csv",
                data_content=response.text  # .text is used for strings/text
            )
            print(f"  -> Success: Saved {len(data['results'])} records to {filename}")
        else:
            print(f"  -> No data found for {year}.")
    else:
        print(f"  -> API Error {response.status_code}: {response.text}")
        
    time.sleep(1)

print("Batch CSV ingestion complete!")