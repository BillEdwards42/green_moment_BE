# ==============================================================================
# SCRIPT: fetch_weather_forecasts.py
# PURPOSE:
#   - Fetches full weather forecasts for all vital counties.
#   - Saves the raw JSON data to a local cache directory.
#   - NEW: Generates a structural fingerprint of the fetched data and logs changes.
# SCHEDULE: Run every 6 hours (e.g., at 00:05, 06:05, 12:05, 18:05).
# ==============================================================================
import os
import requests
import json
from pathlib import Path
from dotenv import load_dotenv
import hashlib
from datetime import datetime
import csv

# --- Configuration ---
load_dotenv()
CWA_API_KEY = os.getenv("CWA_API_KEY")
if not CWA_API_KEY:
    CWA_API_KEY = "CWA-498C56D4-B151-4539-992D-B2CB97042454"

BASE_API_URL = "https://opendata.cwa.gov.tw/api/v1/rest/datastore/F-D0047-093"

# Final map of all vital counties and their CWA Location IDs
VITAL_LOCATIONS_MAP = {
    '臺北市': 'F-D0047-063', '新北市': 'F-D0047-071', '基隆市': 'F-D0047-051',
    '桃園市': 'F-D0047-007', '苗栗縣': 'F-D0047-015', '臺中市': 'F-D0047-075',
    '彰化縣': 'F-D0047-019', '高雄市': 'F-D0047-067', '臺南市': 'F-D0047-079',
    '屏東縣': 'F-D0047-035', '花蓮縣': 'F-D0047-043', '澎湖縣': 'F-D0047-047'
}

# Define the cache directory within the new project folder
BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "forecast_cache"
STRUCTURE_LOG_FILE = BASE_DIR / "weather_structure_log.txt"
FINGERPRINT_FILE = BASE_DIR / "weather_structure_fingerprint.json"
WEATHER_DATA_LOG_FILE = BASE_DIR / "weather_data_log.csv"

def extract_and_log_weather_data(county_name: str, data: dict):
    """
    Extracts specific weather data points (TEMP, WIND, W_CODE) for 'now' and 'future_12h'
    and logs them to a CSV file.
    """
    log_entry = {
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "county": county_name,
        "TEMP_now": "N/A",
        "WIND_now": "N/A",
        "W_CODE_now": "N/A",
        "TEMP_future_12h": "N/A",
        "WIND_future_12h": "N/A",
        "W_CODE_future_12h": "N/A",
    }

    try:
        # Corrected path to location data
        location_data = data['records']['Locations'][0]['Location'][0]
        weather_elements = location_data['WeatherElement']

        elements_to_extract = {
            "TEMP": {"element_name": "平均溫度", "value_key": "Temperature"},
            "WIND": {"element_name": "風速", "value_key": "WindSpeed"},
            "W_CODE": {"element_name": "天氣現象", "value_key": "WeatherCode"}
        }

        extracted_values = {}

        for key, info in elements_to_extract.items():
            element_name = info["element_name"]
            value_key = info["value_key"]

            for element in weather_elements:
                if element['ElementName'] == element_name:
                    times = element['Time']
                    if times:
                        # "now" - first available time entry
                        if f"{key}_now" not in extracted_values and times[0]['ElementValue']:
                            extracted_values[f"{key}_now"] = times[0]['ElementValue'][0].get(value_key, "N/A")

                        # "future_12h" - second available time entry (as a proxy)
                        if len(times) > 1 and f"{key}_future_12h" not in extracted_values and times[1]['ElementValue']:
                            extracted_values[f"{key}_future_12h"] = times[1]['ElementValue'][0].get(value_key, "N/A")
                    break # Found the element, move to next key

        # Populate log_entry with extracted values
        for key, value in extracted_values.items():
            log_entry[key] = value

    except Exception as e:
        print(f"⚠️ WARNING: Could not extract weather data for {county_name}: {e}")

    # Write to CSV
    file_exists = os.path.exists(WEATHER_DATA_LOG_FILE)
    with open(WEATHER_DATA_LOG_FILE, 'a', newline='', encoding='utf-8') as csvfile:
        fieldnames = list(log_entry.keys())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader() # Write header only if file doesn't exist

        writer.writerow(log_entry)
    print(f"📊 Weather data logged for {county_name} to {WEATHER_DATA_LOG_FILE}")

def generate_structure_fingerprint(data):
    """Generates a hash representing the structure of the JSON data."""
    def _traverse(obj):
        if isinstance(obj, dict):
            return sorted([(k, _traverse(v)) for k, v in obj.items()])
        elif isinstance(obj, list):
            # For lists, we only care about the structure of the first element
            # assuming all elements in a list have the same structure.
            return [_traverse(obj[0])] if obj else []
        else:
            return str(type(obj).__name__)

    structure_string = json.dumps(_traverse(data), sort_keys=True)
    return hashlib.md5(structure_string.encode('utf-8')).hexdigest()

def fetch_and_save_forecast(location_id: str, county_name: str):
    """Fetches all data for a location ID and saves the raw JSON."""
    params = {"Authorization": CWA_API_KEY, "locationId": location_id}

    print(f"📡 Fetching ALL data for {county_name} (ID: {location_id})...")
    
    try:
        response = requests.get(BASE_API_URL, params=params, timeout=30)
        print(f"   -> Request sent. HTTP Status Code: {response.status_code}")
        response.raise_for_status()
        
        data = response.json()
        print(f"✅ SUCCESS: API response received for {county_name}.")
        
        output_file_path = OUTPUT_DIR / f"{county_name}_forecast.json"
        
        print(f"💾 Saving raw JSON to: {output_file_path}")
        with open(output_file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"   -> File for {county_name} has been saved.")
        return data # Return fetched data for fingerprinting

    except requests.exceptions.HTTPError as e:
        print(f"❌ HTTP ERROR for {county_name}: {e}\n   -> Response: {response.text}")
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR for {county_name}: {e}")
    return None

if __name__ == "__main__":
    run_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{run_timestamp}] --- Starting Full Regional Forecast Fetch ---")
    
    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        
        all_fetched_data = []
        for county, loc_id in VITAL_LOCATIONS_MAP.items():
            fetched_data = fetch_and_save_forecast(loc_id, county)
            if fetched_data: # Only add if fetch was successful
                all_fetched_data.append(fetched_data)
                extract_and_log_weather_data(county, fetched_data)
            print("-" * 40)
        
        print("✅ All forecast fetches complete.")

        # --- Structural Integrity Check ---
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if all_fetched_data:
            # Create a combined fingerprint from all fetched data
            combined_fingerprint = generate_structure_fingerprint(all_fetched_data)

            previous_fingerprint = None
            if FINGERPRINT_FILE.exists():
                try:
                    with open(FINGERPRINT_FILE, 'r', encoding='utf-8') as f:
                        previous_fingerprint = json.load(f).get('fingerprint')
                except json.JSONDecodeError:
                    print(f"⚠️ WARNING: Could not decode previous fingerprint from {FINGERPRINT_FILE}. Treating as new structure.")

            if combined_fingerprint != previous_fingerprint:
                log_message = f"[{current_timestamp}] ❌ WEATHER DATA STRUCTURE CHANGE DETECTED!\n"
                log_message += f"  Old Fingerprint: {previous_fingerprint}\n"
                log_message += f"  New Fingerprint: {combined_fingerprint}\n"
                log_message += "  Please review the CWA API documentation or fetched JSON files for changes.\n"
                print(log_message)
                with open(STRUCTURE_LOG_FILE, 'a', encoding='utf-8') as f:
                    f.write(log_message)
                
                # Save the new fingerprint
                with open(FINGERPRINT_FILE, 'w', encoding='utf-8') as f:
                    json.dump({'fingerprint': combined_fingerprint, 'timestamp': current_timestamp}, f, indent=2)
                print(f"   -> New structure fingerprint saved to {FINGERPRINT_FILE}.")
            else:
                log_message = f"[{current_timestamp}] ✅ Weather data structure remains consistent.\n"
                print(log_message)
                with open(STRUCTURE_LOG_FILE, 'a', encoding='utf-8') as f:
                    f.write(log_message)
        else:
            log_message = f"[{current_timestamp}] ⚠️ No weather data fetched successfully to generate a structure fingerprint.\n"
            print(log_message)
            with open(STRUCTURE_LOG_FILE, 'a', encoding='utf-8') as f:
                f.write(log_message)

        end_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{end_timestamp}] --- Weather Fetch and Structure Check Complete ---")
        print(f"[{end_timestamp}] ✅ SCRIPT SUCCEEDED.")

    except Exception as e:
        error_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{error_timestamp}] ❌ SCRIPT FAILED: An unexpected error occurred.")
        print(f"[{error_timestamp}] Error details: {e}")
        # Also log to the structure log file for a persistent record of failure
        with open(STRUCTURE_LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(f"[{error_timestamp}] ❌ SCRIPT FAILED: {e}\n")