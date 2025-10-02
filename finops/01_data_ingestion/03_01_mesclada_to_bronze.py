#!/usr/bin/env python3
"""
Mesclada Vendas Loader - Load Excel file from mesclada folder to bronze.mesclada_vendas
Implements hash-based incremental loading for efficient updates.
Uses the same strategy as clinisys loader for high performance.
"""

import logging
import pandas as pd
import duckdb
from datetime import datetime
import os
import glob

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

# Configuration
DUCKDB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'database', 'huntington_data_lake.duckdb')
DATA_INPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data_input', 'mesclada')
SHEET_NAME = 'Sheet1'
TABLE_NAME = 'mesclada_vendas'

def get_duckdb_connection():
    """Create DuckDB connection"""
    try:
        logger.info(f"Attempting to connect to DuckDB at: {DUCKDB_PATH}")
        con = duckdb.connect(DUCKDB_PATH)
        logger.info("DuckDB connection successful")
        return con
    except Exception as e:
        logger.error(f"Failed to connect to DuckDB: {e}")
        raise

def get_newest_excel_file():
    """Get the newest Excel file from the mesclada directory"""
    excel_files = glob.glob(os.path.join(DATA_INPUT_DIR, "*.xlsx"))
    if not excel_files:
        raise FileNotFoundError(f"No Excel files found in {DATA_INPUT_DIR}")
    
    # Filter out temporary Excel files (files starting with ~$)
    excel_files = [f for f in excel_files if not os.path.basename(f).startswith('~$')]
    
    if not excel_files:
        raise FileNotFoundError(f"No valid Excel files found in {DATA_INPUT_DIR}")
    
    # Get the newest file by modification time
    newest_file = max(excel_files, key=os.path.getmtime)
    logger.info(f"Found newest file: {os.path.basename(newest_file)}")
    return newest_file

def create_bronze_table(con, df):
    """Create the bronze.mesclada_vendas table - drops and recreates for fresh data"""
    logger.info(f"Creating bronze table: {TABLE_NAME}")
    
    # Create bronze schema if it doesn't exist
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    
    # Drop existing table to ensure fresh data
    con.execute(f"DROP TABLE IF EXISTS bronze.{TABLE_NAME}")
    logger.info(f"Dropped existing bronze.{TABLE_NAME} table")
    
    # Create table dynamically based on DataFrame columns
    columns = []
    for col in df.columns:
        # Clean column name for SQL (remove special characters)
        clean_col = col.replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '').replace('%', 'pct').replace('.', '_')
        columns.append(f'"{col}" VARCHAR')
    
    # Add metadata columns
    columns.extend([
        'line_number INTEGER',
        'extraction_timestamp VARCHAR',
        'file_name VARCHAR'
    ])
    
    create_table_sql = f"""
    CREATE TABLE bronze.{TABLE_NAME} (
        {', '.join(columns)}
    )
    """
    
    con.execute(create_table_sql)
    logger.info(f"Table bronze.{TABLE_NAME} created successfully")

def process_excel_file(file_path, con):
    """Process the Excel file and load data to bronze.mesclada_vendas"""
    file_name = os.path.basename(file_path)
    logger.info(f"Processing file: {file_name}")
    
    try:
        # Read Excel file from BD HT sheet with optimized settings
        logger.info(f"Reading sheet '{SHEET_NAME}' from {file_name}")
        
        # Use optimized reading settings
        df = pd.read_excel(
            file_path, 
            sheet_name=SHEET_NAME,
            engine='openpyxl',
            dtype=str  # Read all as strings to avoid type issues
        )
        
        logger.info(f"Read {len(df)} rows from {file_name}")
        
        if len(df) == 0:
            logger.warning(f"No data found in {file_name}")
            return 0
        
        # Clean data - replace empty strings with None
        for col in df.columns:
            df[col] = df[col].replace('', None)
        
        # Create bronze table for this data
        create_bronze_table(con, df)
        
        # Add metadata columns
        extraction_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create new DataFrame with all columns to avoid fragmentation
        new_df = df.copy()
        new_df['line_number'] = new_df.index  # Add line number based on DataFrame index
        new_df['extraction_timestamp'] = extraction_timestamp
        new_df['file_name'] = file_name
        df = new_df
        
        logger.info(f"Inserting {len(df)} rows from {file_name}")
        
        # Use the same efficient strategy as clinisys - direct DataFrame insertion
        # All data is already strings from dtype=str
        con.execute(f"INSERT INTO bronze.{TABLE_NAME} SELECT * FROM df")
        
        logger.info(f"Successfully inserted {len(df)} rows from {file_name}")
        return len(df)
        
    except Exception as e:
        logger.error(f"Error processing file {file_name}: {e}")
        raise

def main():
    """Main function to load mesclada Excel file to bronze.mesclada_vendas"""
    logger.info("Starting Mesclada Vendas data loader")
    logger.info(f"DuckDB path: {DUCKDB_PATH}")
    logger.info(f"Data input directory: {DATA_INPUT_DIR}")
    
    try:
        # Create DuckDB connection
        logger.info("Creating DuckDB connection...")
        con = get_duckdb_connection()
        logger.info("DuckDB connection created successfully")
        
        # Get the newest Excel file
        file_path = get_newest_excel_file()
        
        # Process the file
        new_rows = process_excel_file(file_path, con)
        
        # Get final table statistics
        result = con.execute(f'SELECT COUNT(*) FROM bronze.{TABLE_NAME}').fetchone()
        total_rows = result[0] if result else 0
        
        # Final summary
        logger.info("=" * 50)
        logger.info("LOADING SUMMARY")
        logger.info("=" * 50)
        logger.info(f"File processed: {os.path.basename(file_path)}")
        logger.info(f"Rows loaded to bronze: {new_rows}")
        logger.info(f"Total rows in bronze.{TABLE_NAME}: {total_rows:,}")
        logger.info("=" * 50)
        
        # Close connection
        con.close()
        logger.info("Mesclada Vendas data loader completed")
        
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        raise

if __name__ == "__main__":
    main()
