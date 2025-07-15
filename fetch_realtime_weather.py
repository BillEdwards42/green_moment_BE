# ==============================================================================
# SCRIPT: fetch_realtime_weather.py
# PURPOSE:
#   - Fetches real-time weather observations for power generation-relevant stations.
#   - Designed to be run once by an external scheduler (e.g., cronjob).
#   - Calculates and saves the average Sunshine Duration, Air Temperature, and
#     Wind Speed for 5 predefined regions into separate JSON files.
#   - Logs the individual data for each station from every fetch into a
#     persistent CSV log, flagging any null/missing values.
# ==============================================================================
import os
import requests
import json
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
import csv
import time

# --- Configuration ---
# Load environment variables from a .env file
# 從 .env 文件加載環境變數
load_dotenv()
CWA_API_KEY = os.getenv("CWA_API_KEY")
if not CWA_API_KEY:
    # Fallback to the provided key if not found in environment
    # 如果環境中找不到，則使用預設的 API 金鑰
    CWA_API_KEY = "CWA-498C56D4-B151-4539-992D-B2CB97042454"

# API endpoint for real-time observations from manned stations
# 即時觀測資料 API 端點 (有人駐守站)
BASE_API_URL = "https://opendata.cwa.gov.tw/api/v1/rest/datastore/O-A0003-001"

# Define the base directory for dynamic path creation relative to the script's location
# 定義相對於腳本位置的基礎目錄以動態創建路徑
BASE_DIR = Path(__file__).parent
# Directory to store the averaged regional data
# 存儲區域平均數據的目錄
WEATHER_DATA_DIR = BASE_DIR / "weather_data"
# Log file for individual station data from each fetch
# 每次擷取的各測站獨立數據日誌文件
WEATHER_LOG_FILE = BASE_DIR / "10min_weather_log.csv"

# Stations relevant to power generation, grouped by region
# 與發電相關的氣象站，按區域分組
STATIONS_BY_REGION = {
    "north": ["臺北", "新北", "基隆", "新竹", "新屋", "鞍部"],
    "central": ["臺中", "後龍", "古坑", "田中", "日月潭", "阿里山", "玉山"],
    "south": ["嘉義", "臺南", "永康", "高雄", "恆春"],
    "east": ["宜蘭", "花蓮", "成功", "臺東", "大武"],
    "island": ["澎湖", "金門", "馬祖", "蘭嶼", "東吉島"]
}

# Fields to extract and average
# 要提取和平均的欄位
TARGET_FIELDS = ["SunshineDuration", "AirTemperature", "WindSpeed"]

# --- Helper Functions ---

def safe_float_convert(value):
    """
    Safely converts a value to float, handling CWA's null identifiers.
    安全地將值轉換為浮點數，並處理中央氣象署的無效值標識。
    Returns the float value or None if conversion is not possible.
    返回浮點數值，如果無法轉換則返回 None。
    """
    try:
        # CWA uses specific negative numbers to indicate missing data
        # 中央氣象署使用特定的負數來表示缺失數據
        if float(value) < -90:
            return None
        return float(value)
    except (ValueError, TypeError):
        return None

def fetch_weather_data():
    """
    Fetches the latest weather observation data from the CWA API.
    從中央氣象署 API 擷取最新的氣象觀測資料。
    """
    params = {"Authorization": CWA_API_KEY}
    print(f"📡 [{datetime.now().strftime('%H:%M:%S')}] Fetching real-time weather data from {BASE_API_URL}...")
    try:
        response = requests.get(BASE_API_URL, params=params, timeout=30)
        response.raise_for_status()
        print(f"✅ SUCCESS: API response received. Status: {response.status_code}")
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"❌ HTTP ERROR: {e}\n   -> Response: {response.text}")
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR during fetch: {e}")
    return None

def get_last_obs_time(filepath):
    """
    Reads the last ObsTime from a regional CSV file to avoid duplicates.
    從區域 CSV 檔案中讀取最後的觀測時間以避免重複。
    """
    if not filepath.exists():
        return None
    try:
        with open(filepath, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            last_row = None
            for row in reader:
                last_row = row
            if last_row:
                return last_row.get("ObsTime")
    except (IOError, KeyError, StopIteration) as e:
        # StopIteration can happen on empty files with some csv reader versions
        print(f"⚠️  Could not read last timestamp from {filepath} (file might be empty or corrupt). Error: {e}")
    return None

def process_and_log_data(api_data):
    """
    Processes the fetched data to calculate regional averages,
    append them to regional CSV files, and log individual station data.
    處理擷取的資料，計算區域平均值，將其附加到區域 CSV 檔案，並記錄各測站的數據。
    """
    # CORRECTED PATH: The station data is under the 'records' key for this API endpoint.
    # 修正路徑：此 API 端點的測站資料位於 'records' 鍵下。
    try:
        all_stations_data = api_data['records']['Station']
        if not all_stations_data:
            print("⚠️ WARNING: API response contains no station data.")
            return
    except KeyError:
        print("❌ ERROR: Could not find a valid 'records' or 'Station' structure in the API data. Aborting.")
        return

    # --- 1. Process and Log Individual Station Data ---
    log_rows = []
    processed_stations = {}
    valid_timestamp = None

    for station in all_stations_data:
        station_name = station.get("StationName")
        if not any(station_name in sl for sl in STATIONS_BY_REGION.values()):
            continue # Skip stations not in our list
        
        if not valid_timestamp:
            valid_timestamp = station.get("ObsTime", {}).get("DateTime")

        elements = station.get("WeatherElement", {})
        
        temp = safe_float_convert(elements.get("AirTemperature"))
        wind = safe_float_convert(elements.get("WindSpeed"))
        sunshine = safe_float_convert(elements.get("SunshineDuration"))
        
        has_null = any(v is None for v in [temp, wind, sunshine])
        
        processed_stations[station_name] = {
            "AirTemperature": temp,
            "WindSpeed": wind,
            "SunshineDuration": sunshine
        }

        log_rows.append({
            "Timestamp": station.get("ObsTime", {}).get("DateTime"),
            "StationName": station_name,
            "AirTemperature": temp if temp is not None else 'NULL',
            "WindSpeed": wind if wind is not None else 'NULL',
            "SunshineDuration": sunshine if sunshine is not None else 'NULL',
            "HasNullValue": has_null
        })

    if not log_rows:
        print("⚠️ No relevant stations found in the fetched data. No logs or data files will be updated.")
        return
    
    if not valid_timestamp:
        print("⚠️ Could not determine a valid observation timestamp from the data. Aborting regional processing.")
        return

    # Append to the 10-minute log file
    log_file_exists = WEATHER_LOG_FILE.exists()
    with open(WEATHER_LOG_FILE, 'a', newline='', encoding='utf-8') as csvfile:
        fieldnames = ["Timestamp", "StationName", "AirTemperature", "WindSpeed", "SunshineDuration", "HasNullValue"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not log_file_exists:
            writer.writeheader()
        writer.writerows(log_rows)
    print(f"📝 {len(log_rows)} station records for timestamp {valid_timestamp} appended to {WEATHER_LOG_FILE}")

    # --- 2. Calculate and Append Regional Averages to CSV ---
    for region, station_list in STATIONS_BY_REGION.items():
        output_path = WEATHER_DATA_DIR / f"{region}.csv"
        
        # Check if data for this timestamp already exists
        last_timestamp = get_last_obs_time(output_path)
        if last_timestamp and last_timestamp == valid_timestamp:
            print(f"✅ Data for {region} is already up-to-date (Timestamp: {valid_timestamp}). Skipping append.")
            continue

        regional_averages = {}
        for field in TARGET_FIELDS:
            values = [
                processed_stations[s_name][field] 
                for s_name in station_list 
                if s_name in processed_stations and processed_stations[s_name][field] is not None
            ]
            
            if values:
                average = sum(values) / len(values)
                regional_averages[field] = round(average, 2)
            else:
                regional_averages[field] = None

        csv_row = {
            "ObsTime": valid_timestamp,
            "SunshineDuration": regional_averages.get("SunshineDuration"),
            "AirTemperature": regional_averages.get("AirTemperature"),
            "WindSpeed": regional_averages.get("WindSpeed")
        }

        file_exists = output_path.exists()
        with open(output_path, 'a', newline='', encoding='utf-8') as f:
            fieldnames = ["ObsTime", "SunshineDuration", "AirTemperature", "WindSpeed"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(csv_row)
            
        print(f"💾 New regional average for {region} appended to {output_path}")


# --- Main Execution Block ---
if __name__ == "__main__":
    # Create the output directory if it doesn't exist
    # 如果輸出目錄不存在，則創建它
    WEATHER_DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    print("--- Starting Real-time Weather Fetch ---")
    print(f"Source Directory: {BASE_DIR}")
    print(f"Data will be saved in: {WEATHER_DATA_DIR}")
    print(f"Individual logs will be saved to: {WEATHER_LOG_FILE}")
    
    # Fetch and process the data once.
    # 擷取並處理一次資料。
    api_response_data = fetch_weather_data()
    if api_response_data:
        process_and_log_data(api_response_data)
    
    print("--- Fetch and Log Complete ---")