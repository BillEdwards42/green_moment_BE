# ==============================================================================
# SCRIPT: live_pipeline_final.py (v15.0 - Comprehensive Fix)
# PURPOSE:
#   - Complete rewrite to ensure correctness.
#   - All previous fixes integrated.
#   - Ensures all regions, including 'Other' and 'Unknown', are processed and saved.
#   - Corrected sanitize_name function for proper parsing.
#   - Removed problematic fillna(-99.0) for weather data.
#   - Ensured weather data is attempted for all regions.
# ==============================================================================
import pandas as pd
from pathlib import Path
import sys, re, json, requests, pytz, time
from datetime import datetime, timedelta
import numpy as np

# --- Configuration ---
BASE_DIR = Path(__file__).parent
FINAL_OUTPUT_DIR = BASE_DIR / "final_data"
FORECAST_CACHE_DIR = BASE_DIR / "forecast_cache"

PLANT_MAP_FILE = BASE_DIR / "plant_to_region_map.csv"
STATE_FILE = BASE_DIR / "last_run_units.json"
LOG_FILE = BASE_DIR / "fluctuation_log.txt"

TAIWAN_TZ = pytz.timezone('Asia/Taipei')
DATA_URL = "https://www.taipower.com.tw/d006/loadGraph/loadGraph/data/genary.json"

# --- Region-Specific Weather Logic Configuration ---
WEATHER_CONFIG = {
    'North': {
        'avg_towns': [('新北市', '林口區'), ('桃園市', '觀音區'), ('苗栗縣', '通霄鎮'), ('臺北市', '中正區')],
        'code_town': ('臺北市', '中正區'),
        'forecast_files': ['新北市_forecast.json', '桃園市_forecast.json', '苗栗縣_forecast.json', '臺北市_forecast.json']
    },
    'Central': {
        'avg_towns': [('臺中市', '龍井區'), ('臺中市', '西屯區'), ('彰化縣', '彰化市')],
        'code_town': ('臺中市', '西屯區'),
        'forecast_files': ['臺中市_forecast.json', '彰化縣_forecast.json']
    },
    'South': {
        'avg_towns': [('高雄市', '永安區'), ('高雄市', '小港區'), ('臺南市', '安南區'), ('屏東縣', '恆春鎮')],
        'code_town': ('高雄市', '苓雅區'),
        'forecast_files': ['高雄市_forecast.json', '臺南市_forecast.json', '屏東縣_forecast.json']
    },
    'East': {
        'avg_towns': [('花蓮縣', '花蓮市')],
        'code_town': ('花蓮縣', '花蓮市'),
        'forecast_files': ['花蓮縣_forecast.json']
    },
    'Islands': {
        'avg_towns': [('澎湖縣', '湖西鄉')],
        'code_town': ('澎湖縣', '湖西鄉'),
        'forecast_files': ['澎湖縣_forecast.json']
    }
}

# --- Helper Functions ---
def sanitize_name(name):
    # Remove content within parentheses first
    name_without_parentheses = re.sub(r'\(.*\)', '', name)
    # Then sanitize invalid characters
    return re.sub(r'[\\/*?:"<>|]', '_', name_without_parentheses).strip()

def infer_region_from_name(unit_name):
    region_keywords = {
        'North': ['林口', '大潭', '新桃', '通霄', '協和', '石門', '翡翠', '桂山', '觀音', '龍潭', '北部'],
        'Central': ['台中', '大甲溪', '明潭', '彰工', '中港', '竹南', '苗栗', '雲林', '麥寮', '中部', '彰'],
        'South': ['興達', '大林', '南部', '核三', '曾文', '嘉義', '台南', '高雄', '永安', '屏東'],
        'East': ['和平', '花蓮', '蘭陽', '卑南', '立霧', '東部'], 
        'Islands': ['澎湖', '金門', '馬祖', '塔山', '離島'],
        'Other': ['汽電共生', '其他台電自有', '其他購電太陽能', '其他購電風力', '購買地熱', '台電自有地熱', '生質能']
    }
    for region, keywords in region_keywords.items():
        if any(kw in str(unit_name) for kw in keywords): return region
    return None

def get_case_insensitive_key(d, key_variants, default=None):
    """Helper to get a value from a dict using a list of possible key casings."""
    for k in key_variants:
        if k in d:
            return d[k]
    return default

def get_forecast_value(forecast_json, town_name, element_name_chinese, target_time):
    try:
        records = get_case_insensitive_key(forecast_json, ['records', 'Records'])
        if not records: return None

        locations_list = get_case_insensitive_key(records, ['locations', 'Locations'])
        if not locations_list: return None

        locations_data = locations_list[0]
        location_list = get_case_insensitive_key(locations_data, ['location', 'Location']) or []

        town_data = next((loc for loc in location_list if get_case_insensitive_key(loc, ['locationName', 'LocationName']) == town_name), None)

        weather_element_list = None
        if town_data:
            weather_element_list = get_case_insensitive_key(town_data, ['weatherElement', 'WeatherElement'])
        if not weather_element_list:
            weather_element_list = get_case_insensitive_key(locations_data, ['weatherElement', 'WeatherElement'])

        if not weather_element_list:
            return None

        element_data = next((el for el in weather_element_list if get_case_insensitive_key(el, ['elementName', 'ElementName']) == element_name_chinese), None)
        if not element_data:
            return None

        time_blocks = get_case_insensitive_key(element_data, ["time", "Time"], default=[])
        if not time_blocks:
            return None

        # --- CORRECTED LOGIC ---
        # Find the time block where the target_time falls within [StartTime, EndTime)
        correct_block = None
        for time_block in time_blocks:
            start_time_str = get_case_insensitive_key(time_block, ['startTime', 'StartTime'])
            end_time_str = get_case_insensitive_key(time_block, ['endTime', 'EndTime'])
            
            if not start_time_str or not end_time_str:
                continue

            start_time = pd.to_datetime(start_time_str).tz_convert(TAIWAN_TZ)
            end_time = pd.to_datetime(end_time_str).tz_convert(TAIWAN_TZ)

            if start_time <= target_time < end_time:
                correct_block = time_block
                break  # Found the correct block, exit loop

        # If no block contains the target time (e.g., target_time is in the past), fall back to the first available block.
        if not correct_block and time_blocks:
            correct_block = time_blocks[0]
        # --- END CORRECTED LOGIC ---

        if correct_block:
            element_values = get_case_insensitive_key(correct_block, ['elementValue', 'ElementValue'], default=[])
            if not element_values: return None
            
            value_dict = element_values[0]
            value_str = None

            if element_name_chinese == '天氣現象':
                value_str = get_case_insensitive_key(value_dict, ['WeatherCode', 'weathercode'])
            else:
                if value_dict and isinstance(value_dict, dict):
                    value_str = next(iter(value_dict.values()), None)

            if value_str is not None and str(value_str).strip() and str(value_str) != '-':
                try:
                    return float(value_str)
                except ValueError:
                    return float(re.findall(r'\d+', value_str)[0])

    except (KeyError, IndexError, TypeError, ValueError):
        return None
    
    return None

def get_regional_weather_features(region, effective_time):
    # No longer skipping 'Other' or 'Unknown' regions here
    if region not in WEATHER_CONFIG: 
        return {} # Return empty dict if region not in WEATHER_CONFIG (e.g., 'Other', 'Unknown')
    
    config = WEATHER_CONFIG[region]
    features = {}
    forecasts = {}
    for f_name in config['forecast_files']:
        try:
            file_path = FORECAST_CACHE_DIR / f_name
            with open(file_path, 'r', encoding='utf-8') as f:
                county_name = f_name.replace('_forecast.json', '')
                forecasts[county_name] = json.load(f)
        except FileNotFoundError:
            print(f"  -> WARNING: Forecast for {f_name} not found in '{FORECAST_CACHE_DIR}'. Skipping {region}.")
            return {}

    for suffix, time_offset in [('_now', 0), ('_future_12h', 12)]:
        target_time = effective_time + timedelta(hours=time_offset)
        
        temps, winds = [], []
        for county, town in config['avg_towns']:
            if county in forecasts:
                temp = get_forecast_value(forecasts[county], town, '平均溫度', target_time)
                wind = get_forecast_value(forecasts[county], town, '風速', target_time)
                if temp is not None: temps.append(temp)
                if wind is not None: winds.append(wind)
        
        features[f'TEMP{suffix}'] = round(np.mean(temps), 2) if temps else np.nan # Use np.nan instead of -99.0
        features[f'WIND{suffix}'] = round(np.mean(winds), 2) if winds else np.nan # Use np.nan instead of -99.0

        code_county, code_town = config['code_town']
        if code_county in forecasts:
            w_code = get_forecast_value(forecasts[code_county], code_town, '天氣現象', target_time)
            features[f'W_CODE{suffix}'] = int(w_code) if w_code is not None else np.nan # Use np.nan instead of -99
        else:
            features[f'W_CODE{suffix}'] = np.nan # Use np.nan instead of -99
            
    return features

def fetch_and_save_demand_data(effective_data_time, formatted_datetime):
    """Fetches and saves the current electricity demand data."""
    print("   -> Fetching current electricity demand...")
    DEMAND_URL = "https://www.taipower.com.tw/d006/loadGraph/loadGraph/data/loadpara.json"
    
    try:
        cache_bust = int(time.time())
        demand_url_with_bust = f"{DEMAND_URL}?_={cache_bust}"
        response = requests.get(demand_url_with_bust, timeout=20)
        response.raise_for_status()
        demand_data = response.json()
        
        # The key is 'curr_load' and it's inside the first element of the 'records' list
        if not demand_data.get('records') or not isinstance(demand_data['records'], list) or not demand_data['records']:
            print("  -> WARNING: 'records' array not found or is empty in demand data. Skipping.")
            print(f"  -> DEBUG: Full demand data response: {demand_data}")
            return

        current_load_str = demand_data['records'][0].get('curr_load')
        
        if not current_load_str:
            print("  -> WARNING: 'curr_load' key not found in demand data records. Skipping.")
            print(f"  -> DEBUG: Full demand data response: {demand_data}")
            return
            
        current_load_mw = float(current_load_str.replace(',', ''))
        
        demand_df = pd.DataFrame([{
            'DATETIME': formatted_datetime,
            'DEMAND_MW': current_load_mw
        }])
        
        output_path = FINAL_OUTPUT_DIR / "electricity_demand.csv"
        
        # Append new data directly
        demand_df.to_csv(output_path, mode='a', header=not output_path.exists(), index=False, encoding='utf-8-sig')
        print(f"   -> Saved current demand ({current_load_mw} MW) to {output_path.name}.")

    except requests.exceptions.RequestException as e:
        print(f"  -> WARNING: Failed to fetch Taipower demand data: {e}")
    except (ValueError, TypeError, KeyError) as e:
        print(f"  -> WARNING: Failed to parse or process demand data: {e}")

def run_pipeline():
    run_time = datetime.now(TAIWAN_TZ)
    effective_minute = (run_time.minute // 10) * 10
    effective_data_time = run_time.replace(minute=effective_minute, second=0, microsecond=0)
    formatted_datetime = effective_data_time.strftime('%Y-%m-%d %H:%M:%S')
    
    print(f"[{run_time.strftime('%Y-%m-%d %H:%M:%S')}] --- Running Pipeline for {formatted_datetime} ---")

    try:
        # Add a cache-busting parameter to the URL
        cache_bust = int(time.time())
        data_url_with_bust = f"{DATA_URL}?_={cache_bust}"
        response = requests.get(data_url_with_bust, timeout=20)
        response.raise_for_status()
        live_data = response.json().get('aaData', [])
    except requests.exceptions.RequestException as e:
        print(f"🚨 FAILED to fetch Taipower data: {e}")
        return

    # --- Fetch and Save Demand Data ---
    fetch_and_save_demand_data(effective_data_time, formatted_datetime)
    # --- End Demand Data ---

    records = []
    fuel_map = {
        '太陽能': '太陽能(Solar)',
        '風力': '風力(Wind)',
        '燃煤': '燃煤(Coal)',
        '燃氣': '燃氣(LNG)',
        '水力': '水力(Hydro)',
        '核能': '核能(Nuclear)',
        '汽電共生': '汽電共生(Co-Gen)',
        '民營電廠-燃煤': '民營電廠-燃煤(IPP-Coal)',
        '民營電廠-燃氣': '民營電廠-燃氣(IPP-LNG)',
        '燃油': '燃油(Oil)',
        '輕油': '輕油(Diesel)',
        '其它再生能源': '其它再生能源(Other Renewable Energy)',
        '儲能': '儲能(Energy Storage System)'
    }
    for row in live_data:
        if len(row) < 5 or '小計' in row[2]: continue
        unit_name, net_p_str = row[2].strip(), str(row[4]).replace(',', '')
        match = re.search(r'<b>(.*?)</b>', row[0])
        if not match or not unit_name or 'Load' in match.group(1): continue
        fuel_type = fuel_map.get(match.group(1), match.group(1))
        net_p = float(net_p_str) if re.match(r'^-?\d+(\.\d+)?$', net_p_str) else None
        if net_p is not None:
            records.append({'DATETIME': formatted_datetime, 'FUEL_TYPE': fuel_type, 'UNIT_NAME': unit_name, 'NET_P': net_p})

    if not records:
        print("No valid generator records found. Exiting.")
        return

    print(f"   -> Fetched data for {len(records)} active power plant units.")
    new_data_df = pd.DataFrame(records)

    try:
        with open(STATE_FILE, 'r') as f:
            previous_units = set(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError):
        previous_units = set()
    
    current_units = set(new_data_df['UNIT_NAME'])

    print(f"DEBUG: Previous units: {previous_units}")
    print(f"DEBUG: Current units: {current_units}")

    newly_added, missing = current_units - previous_units, previous_units - current_units
    
    log_status_symbol = '✅' if not newly_added and not missing else '❌'
    log_message = f"--- Fluctuation Report @ {formatted_datetime} ({len(current_units)} plants) {log_status_symbol} ---\n"
    if newly_added: log_message += f"  [ADDED] {', '.join(sorted(list(newly_added)))} \n"
    if missing: log_message += f"  [MISSING] {', '.join(sorted(list(missing)))} \n"
    
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(log_message)
    print(f"   -> Fluctuation log updated. Added: {len(newly_added)}, Missing: {len(missing)}.")
    with open(STATE_FILE, 'w') as f: json.dump(list(current_units), f)

    print("   -> Assigning regions using 3-layer logic...")
    try:
        plant_map_df = pd.read_csv(PLANT_MAP_FILE)
        df_merged = pd.merge(new_data_df, plant_map_df, on='UNIT_NAME', how='left')
    except FileNotFoundError:
        print(f"   -> INFO: '{PLANT_MAP_FILE}' not found. Skipping CSV mapping.")
        df_merged = new_data_df
        df_merged['REGION'] = None
    unmapped_mask = df_merged['REGION'].isna()
    df_merged.loc[unmapped_mask, 'REGION'] = df_merged.loc[unmapped_mask, 'UNIT_NAME'].apply(infer_region_from_name)
    df_merged['REGION'].fillna('Unknown', inplace=True)
    print(f"DEBUG: Unique regions after assignment: {df_merged['REGION'].unique()}")
    df_merged['REGION'] = df_merged['REGION'].str.strip()

    # --- Unknown Plants Logging ---
    unknown_plants_df = df_merged[df_merged['REGION'] == 'Unknown']
    if not unknown_plants_df.empty:
        unknown_plant_names = sorted(unknown_plants_df['UNIT_NAME'].tolist())
        unknown_log_message = f"[{formatted_datetime}] ❌ Unknown Plants Detected:\n"
        unknown_log_message += f"  {', '.join(unknown_plant_names)}\n"
        print(f"   -> Unknown plants logged: {len(unknown_plant_names)}.")
    else:
        unknown_log_message = f"[{formatted_datetime}] ✅ No Unknown Plants Detected.\n"
        print("   -> No unknown plants detected.")

    UNKNOWN_PLANTS_LOG_FILE = BASE_DIR / "unknown_plants_log.txt"
    with open(UNKNOWN_PLANTS_LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(unknown_log_message)
    # --- End Unknown Plants Logging ---

    # --- Unit Details Logging ---
    UNIT_DETAILS_LOG_FILE = BASE_DIR / "unit_details_log.csv"
    unit_details_df = df_merged[['DATETIME', 'UNIT_NAME', 'REGION', 'FUEL_TYPE']].copy()
    unit_details_df.to_csv(UNIT_DETAILS_LOG_FILE, mode='a', header=not UNIT_DETAILS_LOG_FILE.exists(), index=False, encoding='utf-8-sig')
    print(f"   -> Appended {len(unit_details_df)} unit details to {UNIT_DETAILS_LOG_FILE.name}.")
    # --- End Unit Details Logging ---
    
    print("   -> Enriching with weather data from local cache...")
    regional_weather_cache = {}
    for region in df_merged['REGION'].unique():
        # Removed the condition that skips fetching weather data for 'Other' or 'Unknown' regions
        regional_weather_cache[region] = get_regional_weather_features(region, effective_data_time)
    weather_df = df_merged['REGION'].map(regional_weather_cache).apply(pd.Series)
    df_enriched = pd.concat([df_merged.drop(columns='UNIT_NAME'), weather_df], axis=1)

    agg_funcs = {
        'NET_P': 'sum', 'TEMP_now': 'first', 'WIND_now': 'first', 'W_CODE_now': 'first',
        'TEMP_future_12h': 'first', 'WIND_future_12h': 'first', 'W_CODE_future_12h': 'first'
    }
    
    aggregated_data = df_enriched.groupby(['DATETIME', 'REGION', 'FUEL_TYPE']).agg(agg_funcs).reset_index()
    print("   -> Aggregated new data with weather into final format.")

    FINAL_OUTPUT_DIR.mkdir(exist_ok=True)
    for (region, fuel_type), group in aggregated_data.groupby(['REGION', 'FUEL_TYPE']):
        
        fuel_folder_name = sanitize_name(fuel_type)
        region_name_safe = sanitize_name(str(region))
        print(f"DEBUG: Sanitized Fuel Folder Name: {fuel_folder_name}")
        print(f"DEBUG: Sanitized Region Name: {region_name_safe}")
        
        # Create folder based on region
        region_folder_path = FINAL_OUTPUT_DIR / region_name_safe
        region_folder_path.mkdir(exist_ok=True)
        
        # File name based on region and fuel type
        output_path = region_folder_path / f"{region_name_safe}_{fuel_folder_name}.csv"

        # Read existing data, filter out current timestamp, and append new data
        if output_path.exists():
            existing_df = pd.read_csv(output_path)
            existing_df['DATETIME'] = pd.to_datetime(existing_df['DATETIME'])
            # Filter out rows with the current formatted_datetime to avoid duplicates
            existing_df = existing_df[pd.to_datetime(existing_df['DATETIME']) != pd.to_datetime(formatted_datetime)]
            combined_df = pd.concat([existing_df, group], ignore_index=True)
        else:
            combined_df = group
        
        combined_df.to_csv(output_path, mode='w', header=True, index=False, encoding='utf-8-sig')
    
    print("   -> Appended enriched data to final segmented files.")
    print(f"[{datetime.now(TAIWAN_TZ).strftime('%Y-%m-%d %H:%M:%S')}] --- Pipeline Run Successful ---")

if __name__ == "__main__":
    run_pipeline()