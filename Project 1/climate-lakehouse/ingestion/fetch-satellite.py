import requests
import os
from datetime import datetime
from dotenv import load_dotenv

# ---------- CONFIG ----------
# Safely load your API key
load_dotenv()
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_KEY")

# Set up the Landing Zone folder for unstructured data
SAT_FOLDER = "../landing-zone/unstructured/satellite"
os.makedirs(SAT_FOLDER, exist_ok=True)

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
        filename = f"{SAT_FOLDER}/spain_temp_{timestamp}.png"

        with open(filename, "wb") as f:
            f.write(response.content)

        print(f"[Satellite] Success! Saved temperature map: {filename}")

    else:
        print(f"[Satellite] HTTP Error: {response.status_code}")
        print(response.text)

except Exception as e:
    print(f"[Satellite] Error: {e}")

print("Satellite collection complete!")