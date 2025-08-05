#!/usr/bin/env python3
"""
FinOps Data Loader - Load Excel files from matr550 folder to bronze schema
Loads billing data from multiple clinic locations with hash-based deduplication.
"""

import logging
import yaml
import pandas as pd
import duckdb
from datetime import datetime
import hashlib
import os
import glob
from pathlib import Path

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
DUCKDB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'database', 'huntington_data_lake.duckdb')
DATA_INPUT_DIR = os.path.join(os.path.dirname(__file__), 'data_input', 'matr550')
SHEET_NAME = '3-Analitico - Itens'

# Clinic code mapping (corrected)
CLINIC_CODES = {
    'xBH': 'Belo Horizonte',
    'xBR': 'Brasília',
    'xCA': 'Campinas',
    'xIB': 'Ibirapuera',
    'xIB2': 'Ibirapuera 2',
    'xSA': 'Salvador',  # Corrected from Santo André
    'xSJ': 'Santa Joana',  # Corrected from São José dos Campos
    'xVM': 'Vila Mariana',
    'xVM2': 'Vila Mariana 2'
}

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

def get_table_name_from_filename(file_name):
    """Extract table name from filename without dates"""
    # Remove .xlsx extension
    base_name = file_name.replace('.xlsx', '')
    # Remove date range (everything after the last dash)
    if '-' in base_name:
        parts = base_name.split('-')
        # Keep everything except the last part (date range)
        table_name = '-'.join(parts[:-1])
    else:
        table_name = base_name
    return table_name

def create_bronze_table(con, table_name, df):
    """Create the bronze table if it doesn't exist"""
    logger.info(f"Creating bronze table: {table_name}")
    
    # Create bronze schema if it doesn't exist
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    
    # Create table dynamically based on DataFrame columns
    columns = []
    for col in df.columns:
        # Clean column name for SQL
        clean_col = col.replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '').replace('%', 'pct')
        columns.append(f'"{col}" VARCHAR')
    
    # Add metadata columns
    columns.extend([
        'hash VARCHAR',
        'extraction_timestamp VARCHAR',
        'file_name VARCHAR'
    ])
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS bronze."{table_name}" (
        {', '.join(columns)}
    )
    """
    
    con.execute(create_table_sql)
    logger.info(f"Table bronze.{table_name} created/verified")

def get_existing_hashes(con, table_name):
    """Get existing hashes from the bronze table"""
    try:
        result = con.execute(f'SELECT hash FROM bronze."{table_name}"').fetchall()
        existing_hashes = {row[0] for row in result if row[0]}
        logger.info(f"Found {len(existing_hashes)} existing hashes in bronze.{table_name}")
        return existing_hashes
    except Exception as e:
        logger.warning(f"Could not get existing hashes for {table_name}: {e}")
        return set()

def process_excel_file(file_path, con, existing_hashes):
    """Process a single Excel file and load data to bronze"""
    file_name = os.path.basename(file_path)
    table_name = get_table_name_from_filename(file_name)
    logger.info(f"Processing file: {file_name} -> table: {table_name}")
    
    try:
        # Read Excel file with proper header handling
        # Header is in row 1 (index 1), data starts from row 2 (index 2)
        df = pd.read_excel(file_path, sheet_name=SHEET_NAME, header=1)
        
        logger.info(f"Read {len(df)} rows from {file_name}")
        
        if len(df) == 0:
            logger.warning(f"No data found in {file_name}")
            return 0
        
        # Keep all columns as-is (raw layer - no formatting)
        # Clean empty strings that might cause type conversion issues
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].replace('', None)
        
        # Create bronze table for this file
        create_bronze_table(con, table_name, df)
        
        # Add metadata columns
        extraction_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create hash for each row (excluding file_name)
        hash_values = df.apply(lambda row: hashlib.md5(
            str(row.values).encode('utf-8')
        ).hexdigest(), axis=1)
        
        # Add hash and metadata columns
        df['hash'] = hash_values
        df['extraction_timestamp'] = extraction_timestamp
        df['file_name'] = file_name
        
        # Filter out rows that already exist (based on hash)
        new_rows = df[~df['hash'].isin(existing_hashes)]
        
        if len(new_rows) == 0:
            logger.info(f"No new rows to insert for {file_name}")
            return 0
        
        logger.info(f"Inserting {len(new_rows)} new rows from {file_name}")
        
        # Insert only new rows
        con.execute(f'INSERT INTO bronze."{table_name}" SELECT * FROM new_rows')
        
        logger.info(f"Successfully inserted {len(new_rows)} new rows from {file_name}")
        return len(new_rows)
        
    except Exception as e:
        logger.error(f"Error processing file {file_name}: {e}")
        return 0

def main():
    """Main function to load all Excel files to bronze"""
    logger.info("Starting FinOps data loader")
    logger.info(f"DuckDB path: {DUCKDB_PATH}")
    logger.info(f"Data input directory: {DATA_INPUT_DIR}")
    
    try:
        # Create DuckDB connection
        logger.info("Creating DuckDB connection...")
        con = get_duckdb_connection()
        logger.info("DuckDB connection created successfully")
        
        # Find all Excel files in the data input directory
        logger.info("Searching for Excel files...")
        excel_files = glob.glob(os.path.join(DATA_INPUT_DIR, "*.xlsx"))
        logger.info(f"Found {len(excel_files)} Excel files: {[os.path.basename(f) for f in excel_files]}")
        
        if not excel_files:
            logger.error(f"No Excel files found in {DATA_INPUT_DIR}")
            return
        
        logger.info(f"Found {len(excel_files)} Excel files to process")
        
        # Process each file
        total_new_rows = 0
        processed_files = 0
        
        for file_path in excel_files:
            try:
                file_name = os.path.basename(file_path)
                table_name = get_table_name_from_filename(file_name)
                
                # Get existing hashes for this specific table
                existing_hashes = get_existing_hashes(con, table_name)
                
                new_rows = process_excel_file(file_path, con, existing_hashes)
                total_new_rows += new_rows
                processed_files += 1
                
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
                continue
        
        # Final summary
        logger.info("=" * 50)
        logger.info("LOADING SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Files processed: {processed_files}/{len(excel_files)}")
        logger.info(f"New rows inserted: {total_new_rows}")
        
        # Get total rows across all tables
        total_rows = 0
        for file_path in excel_files:
            file_name = os.path.basename(file_path)
            table_name = get_table_name_from_filename(file_name)
            try:
                result = con.execute(f'SELECT COUNT(*) FROM bronze."{table_name}"').fetchone()
                table_rows = result[0] if result else 0
                total_rows += table_rows
                logger.info(f"Table {table_name}: {table_rows} rows")
            except Exception as e:
                logger.warning(f"Could not get row count for {table_name}: {e}")
        
        logger.info(f"Total rows across all tables: {total_rows}")
        logger.info("=" * 50)
        
        # Close connection
        con.close()
        logger.info("FinOps data loader completed")
        
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        raise

if __name__ == "__main__":
    main() 