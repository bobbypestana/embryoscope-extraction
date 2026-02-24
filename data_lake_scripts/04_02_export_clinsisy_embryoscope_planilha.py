#!/usr/bin/env python3
"""
Simplified and Efficient Export to Excel
- Exports ALL columns from gold.planilha_embryoscope_combined
- Filter: oocito_TCD = 'Transferido' OR trat2_resultado_tratamento IS NOT NULL
"""

import duckdb as db
import pandas as pd
from datetime import datetime
import os
import logging

# Setup logging
LOGS_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
script_name = os.path.splitext(os.path.basename(__file__))[0]
LOG_PATH = os.path.join(LOGS_DIR, f'{script_name}_{timestamp}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Output directory
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'data_exports')
os.makedirs(OUTPUT_DIR, exist_ok=True)

def main():
    logger.info("=== STARTING STREAMLINED EXCEL EXPORT ===")
    
    # DB setup
    repo_root = os.path.dirname(os.path.dirname(__file__))
    db_path = os.path.join(repo_root, 'database', 'huntington_data_lake.duckdb')
    
    try:
        con = db.connect(db_path, read_only=True)
        
        # New Filter (Clinical Success/Transfer)
        where_clause = "WHERE oocito_TCD IN ('Transferido', 'Criopreservado')"
        
        logger.info("Reading data from gold.planilha_embryoscope_combined...")
        df = con.execute(f"SELECT * FROM gold.planilha_embryoscope_combined {where_clause}").df()
        
        total_rows = len(df)
        total_cols = len(df.columns)
        logger.info(f"Loaded {total_rows:,} rows and {total_cols} columns")
        
        if total_rows == 0:
            logger.warning("No data matches the filter criteria. Skipping export.")
            return

        # Prepare output filename
        output_filename = f'planilha_embryoscope_combined_{timestamp}.xlsx'
        output_path = os.path.join(OUTPUT_DIR, output_filename)
        
        logger.info(f"Writing all data to {output_path}...")
        
        # Using xlsxwriter for performance. No column filtering as requested.
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='CombinedData', index=False)
            
            # Simple Column Formatting
            worksheet = writer.sheets['CombinedData']
            # Set a standard width for all columns (faster than auto-adjusting 222 columns)
            worksheet.set_column(0, total_cols - 1, 15)
            
        logger.info("Excel export completed successfully.")
        
        # File size info
        file_size = os.path.getsize(output_path) / (1024 * 1024)
        logger.info(f"Final file size: {file_size:.2f} MB")
        
    except Exception as e:
        logger.error(f"Error during export: {e}")
        raise
    finally:
        if 'con' in locals():
            con.close()

if __name__ == "__main__":
    main()
