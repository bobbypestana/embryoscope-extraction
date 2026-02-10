"""
Step 4: Track Changes to Gold
Compares current Bronze data with existing Silver to detect status changes.
Records significant changes (e.g., Error -> Success) in Gold audit table.
"""

import sys
import os
import duckdb
from pathlib import Path
import pandas as pd
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Resolve paths relative to script location
SCRIPT_DIR = Path(__file__).parent
DB_PATH = str((SCRIPT_DIR / "../../database/huntington_data_lake.duckdb").resolve())


def track_status_changes():
    """Detect and record status changes between old Silver and new Bronze data."""
    logger.info("Connecting to database...")
    conn = duckdb.connect(DB_PATH)
    
    # Find status changes
    logger.info("Detecting status changes...")
    changes = conn.execute("""
        WITH latest_bronze AS (
            SELECT 
                embryo_EmbryoID,
                api_response_status,
                api_response_code,
                ROW_NUMBER() OVER (PARTITION BY embryo_EmbryoID ORDER BY checked_at DESC, log_id DESC) as rn
            FROM bronze.embryo_image_availability_logs
        ),
        bronze_latest AS (
            SELECT * FROM latest_bronze WHERE rn = 1
        )
        SELECT 
            b.embryo_EmbryoID,
            s.api_response_status as previous_status,
            s.api_response_code as previous_code,
            b.api_response_status as new_status,
            b.api_response_code as new_code,
            CASE 
                WHEN s.embryo_EmbryoID IS NULL THEN 'new'
                WHEN s.api_response_code = 500 AND b.api_response_code = 200 THEN 'error_to_success'
                WHEN s.api_response_code = 200 AND b.api_response_code = 500 THEN 'success_to_error'
                WHEN s.api_response_code = 204 AND b.api_response_code = 200 THEN 'no_images_to_success'
                WHEN s.api_response_code != b.api_response_code THEN 'code_change'
                ELSE 'status_change'
            END as change_type
        FROM bronze_latest b
        LEFT JOIN silver.embryo_image_availability_latest s
            ON b.embryo_EmbryoID = s.embryo_EmbryoID
        WHERE s.embryo_EmbryoID IS NULL 
           OR s.api_response_code != b.api_response_code
           OR s.api_response_status != b.api_response_status
    """).df()
    
    if len(changes) == 0:
        logger.info("No status changes detected")
        conn.close()
        return
    
    logger.info(f"Found {len(changes):,} status changes")
    
    # Get next change_id
    max_id = conn.execute("SELECT COALESCE(MAX(change_id), 0) FROM gold.embryo_image_status_changes").fetchone()[0]
    changes['change_id'] = range(max_id + 1, max_id + 1 + len(changes))
    
    # Insert changes
    logger.info("Recording changes in Gold...")
    conn.execute("INSERT INTO gold.embryo_image_status_changes SELECT * FROM changes")
    
    # Verify
    total_changes = conn.execute("SELECT COUNT(*) FROM gold.embryo_image_status_changes").fetchone()[0]
    
    conn.close()
    
    logger.info(f"✓ Successfully recorded status changes")
    logger.info(f"  New changes: {len(changes):,}")
    logger.info(f"  Total changes tracked: {total_changes:,}")
    
    # Summary by change type
    summary = changes.groupby('change_type').size().reset_index(name='count')
    print("\nChange Summary:")
    print(summary.to_string(index=False))


def main():
    logger.info("="*80)
    logger.info("STEP 4: Tracking Status Changes to Gold")
    logger.info("="*80)
    
    track_status_changes()
    
    logger.info("\n" + "="*80)
    logger.info("CHANGE TRACKING COMPLETE!")
    logger.info("Pipeline finished successfully!")
    logger.info("="*80)


if __name__ == "__main__":
    main()
