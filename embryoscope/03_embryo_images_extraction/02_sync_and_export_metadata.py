"""
Script to synchronize embryo extraction results from temporary logs to DuckDB
and export metadata/clinical reports to Excel.

Usage:
    python 02_sync_and_export_metadata.py
"""

import os
import sys
import logging
import duckdb
import pandas as pd
from datetime import datetime
import glob

# Add parent directory to path to import utils
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import image_extraction_utils as utils

# Setup logging
LOGS_DIR = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
LOG_PATH = os.path.join(LOGS_DIR, f'metadata_sync_{timestamp}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Configuration
DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'database', 'huntington_data_lake.duckdb')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'export_images')
os.makedirs(OUTPUT_DIR, exist_ok=True)

def main():
    try:
        logger.info("=" * 80)
        logger.info("METADATA SYNCHRONIZATION AND EXPORT")
        logger.info("=" * 80)
        
        # 1. FIND LOG FILES
        log_pattern = os.path.join(LOGS_DIR, 'extraction_results_*.jsonl')
        log_files = glob.glob(log_pattern)
        
        if not log_files:
            logger.info("No extraction result logs found to synchronize.")
        else:
            logger.info(f"Found {len(log_files)} log file(s) to process.")
            
            # 2. SYNCHRONIZE EACH LOG FILE
            conn = duckdb.connect(DB_PATH)
            try:
                for log_file in log_files:
                    logger.info(f"Processing: {os.path.basename(log_file)}")
                    utils.sync_results_log_to_db(conn, log_file)
            finally:
                conn.close()
                logger.info("Database synchronization complete.")

        # 3. EXPORT METADATA TO EXCEL
        try:
            logger.info("\n" + "-" * 40)
            logger.info("EXPORTING METADATA REPORT")
            logger.info("-" * 40)
            
            conn = duckdb.connect(DB_PATH)
            try:
                metadata_query = 'SELECT * FROM gold.embryo_images_metadata ORDER BY extraction_timestamp DESC'
                metadata_df = conn.execute(metadata_query).df()
                
                if not metadata_df.empty:
                    metadata_excel_path = os.path.join(OUTPUT_DIR, f'00_extraction_metadata_{timestamp}.xlsx')
                    metadata_df.to_excel(metadata_excel_path, index=False)
                    logger.info(f"[OK] Metadata exported to: {metadata_excel_path}")
                    logger.info(f"  Total records: {len(metadata_df)}")
                else:
                    logger.info("Metadata table is empty, skipping export.")
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Failed to export metadata to Excel: {e}")

        # 4. EXPORT CLINICAL DATA TO EXCEL
        try:
            logger.info("\n" + "-" * 40)
            logger.info("EXPORTING CLINICAL DATA REPORT")
            logger.info("-" * 40)
            
            # Query IDs that have successful extractions
            conn = duckdb.connect(DB_PATH)
            try:
                # Get Slide IDs from the metadata table that are 'success'
                success_query = "SELECT DISTINCT embryo_id FROM gold.embryo_images_metadata WHERE status = 'success'"
                success_ids = [row[0] for row in conn.execute(success_query).fetchall()]
                
                if success_ids:
                    ids_str = "', '".join(success_ids)
                    clinical_query = f'SELECT * FROM gold.data_ploidia WHERE "Slide ID" IN (\'{ids_str}\')'
                    clinical_df = conn.execute(clinical_query).df()
                    
                    if not clinical_df.empty:
                        clinical_excel_path = os.path.join(OUTPUT_DIR, f'01_clinical_data_{timestamp}.xlsx')
                        clinical_df.to_excel(clinical_excel_path, index=False)
                        logger.info(f"[OK] Clinical data exported to: {clinical_excel_path}")
                        logger.info(f"  Total records: {len(clinical_df)}")
                    else:
                        logger.info("No matching clinical data found for successful extractions.")
                else:
                    logger.info("No successful extractions found in metadata, skipping clinical data export.")
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Failed to export clinical data to Excel: {e}")

    except Exception as e:
        logger.error(f"Fatal error in synchronization workflow: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("\nWorkflow finished.")
        logger.info("=" * 80)

if __name__ == '__main__':
    main()
