# ==============================================================================
# SCRIPT: verify_pipeline_output.py
# PURPOSE:
#   - Reads the final, enriched data from the 'final_data' directory.
#   - Finds the latest timestamp and aggregates the generation data.
#   - Prints a formatted report of the current generation mix.
# ==============================================================================
import pandas as pd
from pathlib import Path
import sys

# --- Configuration ---
# ❗ UPDATED: Pointing to the new 'actual' project directory
BASE_DIR = Path.home() / "Programs/良辰電時/actual"
# ❗ UPDATED: Pointing to the final output directory of the new pipeline
FINAL_DATA_DIR = BASE_DIR / "final_data"

def verify_aggregation():
    """
    Finds, loads, and verifies the latest data entries from the final CSV files.
    """
    run_time = pd.Timestamp.now(tz='Asia/Taipei')
    print(f"[{run_time.strftime('%Y-%m-%d %H:%M:%S')}] --- Starting Verification Script ---")

    if not FINAL_DATA_DIR.exists():
        print(f"🚨 ERROR: Final data directory not found at '{FINAL_DATA_DIR}'.")
        sys.exit(1)

    all_csv_files = list(FINAL_DATA_DIR.glob('**/*.csv'))
    if not all_csv_files:
        print(f"🚨 ERROR: No CSV files found in '{FINAL_DATA_DIR}'.")
        sys.exit(1)
        
    print(f"   -> Found {len(all_csv_files)} final data files to analyze.")
    
    try:
        # This will load all columns, including the new weather features
        df_list = [pd.read_csv(f) for f in all_csv_files]
        combined_df = pd.concat(df_list, ignore_index=True)
        combined_df['DATETIME'] = pd.to_datetime(combined_df['DATETIME'])
    except Exception as e:
        print(f"🚨 ERROR: Failed to load or process CSV files. Details: {e}")
        sys.exit(1)

    if combined_df.empty:
        print("🚨 ERROR: All data files are empty. Nothing to verify.")
        sys.exit(1)
        
    # Find the most recent timestamp in the entire dataset
    latest_timestamp = combined_df['DATETIME'].max()
    latest_df = combined_df[combined_df['DATETIME'] == latest_timestamp].copy()
    
    if latest_df.empty:
        print(f"🚨 ERROR: No data found for the latest timestamp ({latest_timestamp}).")
        sys.exit(1)

    # The core logic remains the same: group by fuel type and sum the power
    fuel_sums = latest_df.groupby('FUEL_TYPE')['NET_P'].sum()
    total_generation = fuel_sums.sum()

    # --- Print the formatted report ---
    print("\n" + "="*50)
    print("台電系統各機組發電量（單位 MW）")
    print(f"更新時間 - {latest_timestamp.strftime('%Y-%m-%d %H:%M')}")
    print("\n各能源別即時發電量小計(每10分鐘更新)：")
    print(f"總計： {total_generation:,.1f} MW\n")

    # Define a preferred order for the report display
    preferred_order = [
        '核能(Nuclear)', '燃煤(Coal)', '汽電共生(Co-Gen)', '民營電廠-燃煤(IPP-Coal)',
        '燃氣(LNG)', '民營電廠-燃氣(IPP-LNG)', '燃油(Oil)', '輕油(Diesel)',
        '水力(Hydro)', '風力(Wind)', '太陽能(Solar)', '其它再生能源(Other Renewable Energy)',
        '儲能(Energy Storage System)'
    ]
    
    # Sort the results according to the preferred order
    sorted_fuel_keys = sorted(
        fuel_sums.keys(), 
        key=lambda x: preferred_order.index(x) if x in preferred_order else len(preferred_order)
    )

    for fuel_type in sorted_fuel_keys:
        total_mw = fuel_sums.get(fuel_type, 0.0)
        percentage = (total_mw / total_generation) * 100 if total_generation > 0 else 0
        
        # Using f-strings for cleaner formatting
        print(f"{fuel_type}")
        print(f"{total_mw:,.1f}")
        print(f"{percentage:.3f}%\n")
        
    print("="*50 + "\n")
    print("✅ Verification report complete.")

def display_latest_fluctuation_report():
    log_file_path = BASE_DIR / "fluctuation_log.txt"
    if not log_file_path.exists():
        print("\n--- Fluctuation Log ---")
        print("No fluctuation log found.")
        return

    with open(log_file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    latest_report_lines = []
    found_latest = False
    for i in reversed(range(len(lines))):
        if "--- Fluctuation Report @" in lines[i]:
            found_latest = True
            latest_report_lines.insert(0, lines[i])
            break
        if found_latest or lines[i].strip(): # Include non-empty lines before the header
            latest_report_lines.insert(0, lines[i])

    print("\n" + "="*50)
    print("--- Latest Fluctuation Report ---")
    if latest_report_lines:
        for line in latest_report_lines:
            print(line.strip())
    else:
        print("No fluctuation reports found.")
    print("="*50)

if __name__ == "__main__":
    verify_aggregation()
    display_latest_fluctuation_report()