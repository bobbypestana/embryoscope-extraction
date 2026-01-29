"""
Log Extraction Script - Recover Embryo Image Availability Data
Parses the large log file to extract results and save to DuckDB.
"""

import sys
import os
import re
from pathlib import Path
import pandas as pd
import duckdb
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Official API Response Codes
API_STATUS_MAP = {
    200: "OK",
    204: "No images found (Empty response)",
    400: "Missing parameter",
    401: "Unauthorized / Invalid Token",
    500: "Unexpected error during data access",
    503: "API service/database busy",
    0: "Not Checked"
}

def extract_from_log(log_path):
    logger.info(f"Opening log file: {log_path}")
    
    # Patterns
    # 2026-01-27 17:05:38,466 - embryoscope_api_Ibirapuera - INFO - Successfully retrieved 871 image runs for D2021.01.28_S01498_I3166_P-8
    success_pattern = re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - embryoscope_api_(.*?) - INFO - Successfully retrieved (\d+) image runs for (.*)')
    
    # Attempts (to find embryos that produced no follow-up line)
    # 2026-01-27 17:05:27,595 - embryoscope_api_Belo Horizonte - INFO - Requesting image runs for embryo D2019.07.17_S00065_I3254_P-1
    request_pattern = re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - embryoscope_api_(.*?) - INFO - Requesting image runs for embryo (.*)')

    # Errors
    # 2026-01-28 05:48:40,164 - embryoscope_api_Ibirapuera - ERROR - [RETRY] Exception on attempt 1: 500 Server Error: Error during image run lookup for url: https://10.250.110.208:4000/GET/imageruns?EmbryoID=D2025.12.14_S0
    # Pattern to capture status code from error
    error_pattern = re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - embryoscope_api_(.*?) - ERROR - .*?(\d{3}) .*? Error.*?EmbryoID=(.*)')

    results = []
    
    with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            # Try success pattern
            match = success_pattern.search(line)
            if match:
                timestamp, unit, count, embryo_id = match.groups()
                results.append({
                    'embryo_EmbryoID': embryo_id.strip(),
                    'image_available': int(count) > 0,
                    'image_runs_count': int(count),
                    'api_response_status': 'success',
                    'api_response_code': 200,
                    'error_message': None,
                    'checked_at': timestamp,
                    'patient_unit_huntington': unit
                })
                continue
                
            # Try request pattern (track all attempts)
            match = request_pattern.search(line)
            if match:
                timestamp, unit, embryo_id = match.groups()
                results.append({
                    'embryo_EmbryoID': embryo_id.strip(),
                    'image_available': False,
                    'image_runs_count': 0,
                    'api_response_status': 'requested',
                    'api_response_code': 0, # Placeholder
                    'error_message': 'Attempted but no result logged',
                    'checked_at': timestamp,
                    'patient_unit_huntington': unit
                })
                continue

            # Try error pattern
            match = error_pattern.search(line)
            if match:
                timestamp, unit, code, embryo_id = match.groups()
                results.append({
                    'embryo_EmbryoID': embryo_id.strip(),
                    'image_available': False,
                    'image_runs_count': 0,
                    'api_response_status': 'error',
                    'api_response_code': int(code),
                    'error_message': f'API Error {code} in logs',
                    'checked_at': timestamp,
                    'patient_unit_huntington': unit
                })

    logger.info(f"Extracted {len(results):,} records from log.")
    return pd.DataFrame(results)

def main():
    log_dir = Path('embryoscope/report/logs')
    db_path = Path('database/huntington_data_lake.duckdb')
    
    # Process all log files in the directory
    log_files = list(log_dir.glob('*.log'))
    logger.info(f"Found {len(log_files)} log files to process.")
    
    all_extracted_dfs = []
    for log_file in log_files:
        df = extract_from_log(log_file)
        if not df.empty:
            all_extracted_dfs.append(df)
            
    if not all_extracted_dfs:
        logger.error("No data extracted from any log file!")
        return
        
    results_df = pd.concat(all_extracted_dfs, ignore_index=True)

    # Deduplicate (keep last checked if multiple)
    # Important: sort so that success/error overwrites simple 'requested'
    results_df['status_priority'] = results_df['api_response_status'].map({
        'success': 3,
        'error': 2,
        'requested': 1
    })
    results_df = results_df.sort_values(['embryo_EmbryoID', 'status_priority', 'checked_at'], ascending=[True, True, True])
    results_df = results_df.drop_duplicates('embryo_EmbryoID', keep='last')
    
    # For those that remained as 'requested', mark as 204 (inferred silent response)
    results_df.loc[results_df['api_response_status'] == 'requested', 'api_response_code'] = 204
    results_df.loc[results_df['api_response_status'] == 'requested', 'api_response_status'] = 'silent_response'
    
    # Map descriptions based on official docs
    def get_status_msg(code):
        return API_STATUS_MAP.get(code, "Unknown response code")
    
    results_df['error_message'] = results_df['api_response_code'].map(get_status_msg)
    # Success specifically doesn't need an "error message" in the final display maybe,
    # but the user said "Map any other responses as Unknown", implying the message field.
    # Let's keep the message field descriptive.

    logger.info(f"After deduplication and tracking attempts: {len(results_df):,} unique embryos.")

    # Connect to DB to get missing clinical info
    logger.info("Enriching with clinical data from database...")
    conn = duckdb.connect(str(db_path))
    
    # Load clinical info from silver/gold
    embryo_info = conn.execute("""
        SELECT 
            prontuario,
            patient_PatientID,
            patient_PatientIDx,
            patient_unit_huntington,
            treatment_TreatmentName,
            embryo_EmbryoID,
            embryo_EmbryoDate
        FROM gold.embryoscope_embrioes
        WHERE embryo_EmbryoID IS NOT NULL
    """).df()
    
    # Normalize patient_unit_huntington in results if needed
    # (The database has names like 'Ibirapuera', logs might have 'embryoscope_api_Ibirapuera')
    results_df['patient_unit_huntington'] = results_df['patient_unit_huntington'].str.replace('embryoscope_api_', '')
    
    # Merge - Use LEFT JOIN to include ALL embryos
    final_df = pd.merge(
        embryo_info,
        results_df[['embryo_EmbryoID', 'image_available', 'image_runs_count', 'api_response_status', 'api_response_code', 'error_message', 'checked_at']],
        on='embryo_EmbryoID',
        how='left'
    )
    
    # Fill missing values for embryos not found in logs
    final_df['api_response_status'] = final_df['api_response_status'].fillna('not_checked')
    final_df['image_available'] = final_df['image_available'].fillna(False)
    final_df['image_runs_count'] = final_df['image_runs_count'].fillna(0)
    # response code 0 or NULL for not checked
    final_df['api_response_code'] = final_df['api_response_code'].fillna(0).astype(int)
    
    # Update error message for not checked
    final_df.loc[final_df['api_response_status'] == 'not_checked', 'error_message'] = API_STATUS_MAP[0]
    
    logger.info(f"Final dataset: {len(final_df):,} records.")
    
    # Save to gold
    logger.info("Saving to gold.embryo_image_availability_raw...")
    conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
    conn.execute("DROP TABLE IF EXISTS gold.embryo_image_availability_raw")
    conn.execute("CREATE TABLE gold.embryo_image_availability_raw AS SELECT * FROM final_df")
    
    # Check counts
    summary = conn.execute("""
        SELECT 
            patient_unit_huntington,
            COUNT(*) as total,
            SUM(CASE WHEN image_available THEN 1 ELSE 0 END) as with_images
        FROM gold.embryo_image_availability_raw
        GROUP BY 1 ORDER BY 1
    """).df()
    print("\nExtraction Summary:")
    print(summary.to_string(index=False))
    
    conn.close()
    logger.info("Done!")

if __name__ == "__main__":
    main()
