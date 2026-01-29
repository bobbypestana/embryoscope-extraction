"""
Step 2: Logs to Bronze
Reads JSON result files from API checks and ingests them into Bronze table.
Bronze is append-only - all historical checks are preserved.
"""

import sys
import os
from pathlib import Path
import json
import pandas as pd
import duckdb
from datetime import datetime
import logging
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Resolve paths relative to script location
SCRIPT_DIR = Path(__file__).parent
DB_PATH = str((SCRIPT_DIR / "../../database/huntington_data_lake.duckdb").resolve())


def ingest_json_to_bronze(json_dir: str):
    """Ingest all JSON files from a directory into Bronze."""
    logger.info(f"Scanning directory: {json_dir}")
    
    # Find all JSON files
    json_files = list(Path(json_dir).glob('*.json'))
    
    if not json_files:
        logger.warning("No JSON files found!")
        return
    
    logger.info(f"Found {len(json_files)} JSON files")
    
    # Load all results
    all_results = []
    for json_file in json_files:
        logger.info(f"Loading {json_file.name}...")
        with open(json_file, 'r') as f:
            data = json.load(f)
            all_results.extend(data)
    
    logger.info(f"Loaded {len(all_results):,} total records")
    
    # Convert to DataFrame
    df = pd.DataFrame(all_results)
    
    # Prepare Bronze data (only columns needed for Bronze)
    bronze_df = df[[
        'embryo_EmbryoID', 'prontuario', 'patient_unit_huntington',
        'image_available', 'image_runs_count',
        'api_response_code', 'error_message', 'checked_at'
    ]].copy()
    
    # Add api_response_status based on code
    bronze_df['api_response_status'] = bronze_df['api_response_code'].map({
        200: 'success',
        204: 'no_content',
        500: 'error',
        0: 'not_checked'
    }).fillna('unknown')
    
    # Connect to database
    conn = duckdb.connect(DB_PATH)
    
    # Get next log_id
    max_id = conn.execute("SELECT COALESCE(MAX(log_id), 0) FROM bronze.embryo_image_availability_logs").fetchone()[0]
    bronze_df['log_id'] = range(max_id + 1, max_id + 1 + len(bronze_df))
    
    # Reorder columns to match Bronze table schema
    bronze_df = bronze_df[[
        'log_id', 'embryo_EmbryoID', 'prontuario', 'patient_unit_huntington',
        'image_available', 'image_runs_count', 'api_response_status',
        'api_response_code', 'error_message', 'checked_at'
    ]]
    
    # Insert into Bronze
    logger.info("Inserting into Bronze...")
    conn.execute("""
        INSERT INTO bronze.embryo_image_availability_logs 
        (log_id, embryo_EmbryoID, prontuario, patient_unit_huntington,
         image_available, image_runs_count, api_response_status,
         api_response_code, error_message, checked_at)
        SELECT * FROM bronze_df
    """)
    
    # Verify
    new_count = conn.execute("SELECT COUNT(*) FROM bronze.embryo_image_availability_logs WHERE log_id > ?", [max_id]).fetchone()[0]
    
    conn.close()
    
    logger.info(f"✓ Successfully inserted {new_count:,} records into Bronze")
    logger.info(f"  Total Bronze records: {max_id + new_count:,}")
    
    # Summary by status
    summary = bronze_df.groupby('api_response_code').size().reset_index(name='count')
    print("\nIngestion Summary:")
    print(summary.to_string(index=False))


def main():
    parser = argparse.ArgumentParser(description='Ingest API results from JSON to Bronze')
    parser.add_argument('--input-dir', required=True, help='Directory containing JSON result files')
    args = parser.parse_args()
    
    logger.info("="*80)
    logger.info("STEP 2: Ingesting JSON results to Bronze")
    logger.info("="*80)
    
    ingest_json_to_bronze(args.input_dir)
    
    logger.info("\n" + "="*80)
    logger.info("BRONZE INGESTION COMPLETE!")
    logger.info("Next step: Run 03_bronze_to_silver.py to update Silver")
    logger.info("="*80)


if __name__ == "__main__":
    main()
