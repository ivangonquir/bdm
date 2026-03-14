import requests
import os
from datetime import datetime

# ---------- CONFIG ----------
CITY = "Barcelona"
ELTIEMPO_FOLDER = "../landing-zone/unstructured/eltiempo"
os.makedirs(ELTIEMPO_FOLDER, exist_ok=True)

print(f"Starting ElTiempo.es scraping for {CITY}...")

# ---------- SINGLE COLLECTION EXECUTION ----------
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

try:
    url_eltiempo = f"https://www.eltiempo.es/{CITY.lower()}.html"
    headers = {'User-Agent': 'Mozilla/5.0'} 
    response = requests.get(url_eltiempo, headers=headers)

    if response.status_code == 200:
        filename_eltiempo = f"{ELTIEMPO_FOLDER}/eltiempo_{timestamp}.html"
        with open(filename_eltiempo, "w", encoding="utf-8") as f:
            f.write(response.text)
        print(f"[ElTiempo] Saved: {filename_eltiempo}")
    else:
        print(f"[ElTiempo] HTTP Error: {response.status_code}")

except Exception as e:
    print(f"[ElTiempo] Error: {e}")

print("ElTiempo collection complete!")