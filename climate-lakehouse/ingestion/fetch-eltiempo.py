import requests
import os
import sys
from datetime import datetime
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)
from landing_zone.minio_client import upload_to_bronze


# ---------- CONFIG ----------
CITY = "Barcelona"
ELTIEMPO_FOLDER = "../landing-zone/unstructured/eltiempo"
# os.makedirs(ELTIEMPO_FOLDER, exist_ok=True)

print(f"Starting ElTiempo.es scraping for {CITY}...")

# ---------- SINGLE COLLECTION EXECUTION ----------
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

try:
    url_eltiempo = f"https://www.eltiempo.es/{CITY.lower()}.html"
    headers = {'User-Agent': 'Mozilla/5.0'} 
    response = requests.get(url_eltiempo, headers=headers)

    if response.status_code == 200:
        filename_eltiempo = f"{ELTIEMPO_FOLDER}/eltiempo_{timestamp}.html"

        upload_to_bronze(
            source_name="ElTiempo",
            data_type="unstructured",
            format_extension="html",
            data_content=response.text 
        )
        """
        with open(filename_eltiempo, "w", encoding="utf-8") as f:
            f.write(response.text)
        print(f"[ElTiempo] Saved: {filename_eltiempo}")
        """
    else:
        print(f"[ElTiempo] HTTP Error: {response.status_code}")

except Exception as e:
    print(f"[ElTiempo] Error: {e}")

print("ElTiempo collection complete!")