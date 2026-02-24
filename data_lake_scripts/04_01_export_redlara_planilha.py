#!/usr/bin/env python3
"""
04_01_export_redlara_planilha.py
Exports the gold.redlara_planilha_combined table to Excel.
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
    logger.info("=== STARTING REDLARA-PLANILHA EXCEL EXPORT ===")
    
    # DB setup
    repo_root = os.path.dirname(os.path.dirname(__file__))
    db_path = os.path.join(repo_root, 'database', 'huntington_data_lake.duckdb')
    
    try:
        con = db.connect(db_path, read_only=True)
        
        logger.info("Reading data from gold.redlara_planilha_combined...")
        df = con.execute("SELECT * FROM gold.redlara_planilha_combined").df()
        
        total_rows = len(df)
        total_cols = len(df.columns)
        logger.info(f"Loaded {total_rows:,} rows and {total_cols} columns")
        
        if total_rows == 0:
            logger.warning("No data found in gold.redlara_planilha_combined. Skipping export.")
            return

        # Convert datetime columns to date only to avoid timestamp residues in Excel
        logger.info("Converting datetime columns to date-only...")
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.date

        # Prepare output filename
        output_filename = f'redlara_planilha_combined_{timestamp}.xlsx'
        output_path = os.path.join(OUTPUT_DIR, output_filename)
        
        logger.info(f"Writing data to {output_path}...")
        
        # Using xlsxwriter for performance.
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='RedlaraPlanilha', index=False)
            
            # Simple Column Formatting
            worksheet = writer.sheets['RedlaraPlanilha']
            # Set a standard width for all columns
            worksheet.set_column(0, total_cols - 1, 15)
            
        logger.info(f"Excel export completed successfully: {output_filename}")
        
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
