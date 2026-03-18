import requests
import os
import sys
from datetime import datetime
from dotenv import load_dotenv, find_dotenv

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)
from minio_client import upload_to_bronze


# ---------- CONFIG ----------
# Safely load your API key
load_dotenv(find_dotenv())
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_KEY")
if OPENWEATHER_API_KEY:
    OPENWEATHER_API_KEY = OPENWEATHER_API_KEY.strip()


print("Starting OpenWeather temperature map ingestion for Spain...")

# ---------- SINGLE COLLECTION EXECUTION ----------
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

try:
    # OpenWeatherMap Tile API: Layer = temp_new, Z = 5, X = 15, Y = 12 (Spain region)
    url = f"https://tile.openweathermap.org/map/temp_new/5/15/12.png?appid={OPENWEATHER_API_KEY}"

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    # Fetch the image
    response = requests.get(url, headers=headers, timeout=30)

    if response.status_code == 200:
        # Save it as a PNG image
        filename = f"spain_temp_{timestamp}.png"

        upload_to_bronze(
            source_name="Satellite",
            data_type="unstructured", 
            format_extension="png",
            data_content=response.content  # .content is used for binary files like images
        )
        """
        with open(filename, "wb") as f:
            f.write(response.content)
        """

        print(f"[Satellite] Success! Saved temperature map: {filename}")

    else:
        print(f"[Satellite] HTTP Error: {response.status_code}")
        print(response.text)

except Exception as e:
    print(f"[Satellite] Error: {e}")

print("Satellite collection complete!")