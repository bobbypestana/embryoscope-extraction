"""
Step 3: Bronze to Silver
Processes Bronze logs to update Silver with latest status per embryo.
Silver is deduplicated - only the most recent check for each embryo is kept.
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


def update_silver_from_bronze():
    """Update Silver table with latest records from Bronze."""
    logger.info("Connecting to database...")
    conn = duckdb.connect(DB_PATH)
    
    # Get the latest check for each embryo from Bronze
    logger.info("Finding latest checks in Bronze...")
    latest_bronze = conn.execute("""
        WITH ranked AS (
            SELECT 
                b.*,
                ROW_NUMBER() OVER (PARTITION BY b.embryo_EmbryoID ORDER BY b.checked_at DESC, b.log_id DESC) as rn
            FROM bronze.embryo_image_availability_logs b
        )
        SELECT * FROM ranked WHERE rn = 1
    """).df()
    
    logger.info(f"Found {len(latest_bronze):,} unique embryos in Bronze")
    
    # Enrich with clinical data
    logger.info("Enriching with clinical data...")
    enriched = conn.execute("""
        SELECT 
            e.embryo_EmbryoID,
            e.prontuario,
            e.patient_PatientID,
            CAST(e.patient_PatientIDx AS VARCHAR) as patient_PatientIDx,
            e.patient_unit_huntington,
            e.treatment_TreatmentName,
            e.embryo_EmbryoDate,
            b.image_available,
            b.image_runs_count,
            b.api_response_status,
            b.api_response_code,
            b.error_message,
            b.checked_at,
            CURRENT_TIMESTAMP as last_updated
        FROM latest_bronze b
        JOIN gold.embryoscope_embrioes e
            ON b.embryo_EmbryoID = e.embryo_EmbryoID
    """).df()
    
    logger.info(f"Enriched {len(enriched):,} records")
    
    # Get embryos that will be updated
    embryo_ids = enriched['embryo_EmbryoID'].tolist()
    
    # Delete existing records for these embryos
    logger.info(f"Removing existing Silver records for {len(embryo_ids):,} embryos...")
    placeholders = ','.join(['?' for _ in embryo_ids])
    deleted = conn.execute(
        f"DELETE FROM silver.embryo_image_availability_latest WHERE embryo_EmbryoID IN ({placeholders})",
        embryo_ids
    ).fetchone()[0]
    
    logger.info(f"Deleted {deleted:,} old records from Silver")
    
    # Insert new records
    logger.info("Inserting updated records into Silver...")
    conn.execute("INSERT INTO silver.embryo_image_availability_latest SELECT * FROM enriched")
    
    # Verify
    total_silver = conn.execute("SELECT COUNT(*) FROM silver.embryo_image_availability_latest").fetchone()[0]
    
    conn.close()
    
    logger.info(f"✓ Successfully updated Silver")
    logger.info(f"  Records updated: {len(enriched):,}")
    logger.info(f"  Total Silver records: {total_silver:,}")
    
    # Summary
    summary = enriched.groupby('api_response_code').size().reset_index(name='count')
    print("\nSilver Update Summary:")
    print(summary.to_string(index=False))


def main():
    logger.info("="*80)
    logger.info("STEP 3: Updating Silver from Bronze")
    logger.info("="*80)
    
    update_silver_from_bronze()
    
    logger.info("\n" + "="*80)
    logger.info("SILVER UPDATE COMPLETE!")
    logger.info("Next step: Run 04_track_changes_to_gold.py to track status changes")
    logger.info("="*80)


if __name__ == "__main__":
    main()
