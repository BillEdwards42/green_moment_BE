import pandas as pd
from pathlib import Path
import sys

# --- Configuration ---
BASE_DIR = Path(__file__).parent
FINAL_DATA_DIR = BASE_DIR / "final_data"
UNIT_DETAILS_LOG_FILE = BASE_DIR / "unit_details_log.csv"
REPORTS_DIR = BASE_DIR / "reports"

def load_latest_data():
    if not UNIT_DETAILS_LOG_FILE.exists():
        print(f"🚨 ERROR: Unit details log not found at '{UNIT_DETAILS_LOG_FILE}'. Run live_pipeline_final.py first.")
        sys.exit(1)

    try:
        combined_df = pd.read_csv(UNIT_DETAILS_LOG_FILE)
        combined_df['DATETIME'] = pd.to_datetime(combined_df['DATETIME'])
    except Exception as e:
        print(f"🚨 ERROR: Failed to load or process unit details log. Details: {e}")
        sys.exit(1)

    if combined_df.empty:
        print("🚨 ERROR: Unit details log is empty. Nothing to report.")
        sys.exit(1)
        
    latest_timestamp = combined_df['DATETIME'].max()
    latest_df = combined_df[combined_df['DATETIME'] == latest_timestamp].copy()
    
    if latest_df.empty:
        print(f"🚨 ERROR: No data found for the latest timestamp ({latest_timestamp}) in unit details log. Nothing to report.")
        sys.exit(1)

    return combined_df, latest_df, latest_timestamp

def generate_latest_vs_all_units_report(combined_df, latest_df, latest_timestamp):
    print("Generating latest vs all units report...")
    all_unique_units = combined_df['UNIT_NAME'].unique()
    latest_units = latest_df['UNIT_NAME'].unique()

    report_data = []
    for unit in sorted(all_unique_units):
        in_latest = unit in latest_units
        report_data.append({'UNIT_NAME': unit, 'InLatestEntry': in_latest})

    report_df = pd.DataFrame(report_data)
    output_path = REPORTS_DIR / "latest_vs_all_units.csv"
    report_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"Report saved to {output_path}")

def generate_regional_unit_reports(latest_df):
    print("Generating regional unit reports...")
    unique_regions = latest_df['REGION'].unique()

    for region in sorted(unique_regions):
        region_units = latest_df[latest_df['REGION'] == region]['UNIT_NAME'].unique()
        region_df = pd.DataFrame({'UNIT_NAME': sorted(region_units)})
        
        # Sanitize region name for filename
        sanitized_region = region.replace('(', '_').replace(')', '').replace(' ', '_')
        output_path = REPORTS_DIR / f"{sanitized_region}_units.csv"
        region_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"Report for {region} saved to {output_path}")

if __name__ == "__main__":
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    
    combined_df, latest_df, latest_timestamp = load_latest_data()
    
    generate_latest_vs_all_units_report(combined_df, latest_df, latest_timestamp)
    generate_regional_unit_reports(latest_df)
    
    print("All reports generated successfully.")
