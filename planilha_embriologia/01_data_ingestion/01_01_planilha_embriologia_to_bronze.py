#!/usr/bin/env python3
"""
Planilha Embriologia Loader - Load all Excel files from year subfolders to bronze.planilha_embriologia
Reads all Excel files from planilha_embriologia/data_input/YYYY/ folders and loads them to bronze layer.
Reads from the "FET" sheet in each file.
Fully overwrites the bronze table on each run.
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
DATA_INPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data_input')
SHEET_NAME = 'FET'  # Read from FET sheet

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

def get_all_excel_files():
    """Get all Excel files from year subfolders (YYYY), ignoring further subfolders"""
    excel_files = []
    
    # Get all year subfolders (directories named as 4-digit years)
    if not os.path.exists(DATA_INPUT_DIR):
        raise FileNotFoundError(f"Data input directory not found: {DATA_INPUT_DIR}")
    
    # List all items in data_input directory
    for item in os.listdir(DATA_INPUT_DIR):
        item_path = os.path.join(DATA_INPUT_DIR, item)
        
        # Check if it's a directory and looks like a year (4 digits)
        if os.path.isdir(item_path) and item.isdigit() and len(item) == 4:
            logger.info(f"Scanning year folder: {item}")
            
            # Get all Excel files directly in this year folder (not in subfolders)
            year_excel_files = glob.glob(os.path.join(item_path, "*.xlsx"))
            
            # Filter out temporary Excel files (files starting with ~$)
            year_excel_files = [f for f in year_excel_files if not os.path.basename(f).startswith('~$')]
            
            excel_files.extend(year_excel_files)
            logger.info(f"Found {len(year_excel_files)} Excel file(s) in {item}/")
    
    if not excel_files:
        raise FileNotFoundError(f"No Excel files found in year subfolders of {DATA_INPUT_DIR}")
    
    logger.info(f"Total Excel files found: {len(excel_files)}")
    return sorted(excel_files)  # Sort for consistent processing order

def check_sheet_exists(file_path, sheet_name):
    """Check if the specified sheet exists in the Excel file"""
    try:
        xl_file = pd.ExcelFile(file_path, engine='openpyxl')
        return sheet_name in xl_file.sheet_names
    except Exception as e:
        logger.warning(f"Could not check sheets in {os.path.basename(file_path)}: {e}")
        return False

def sanitize_table_name(file_name):
    """Convert file name to a valid SQL table name"""
    # Remove extension
    table_name = os.path.splitext(file_name)[0]
    # Replace spaces and special characters with underscores
    table_name = table_name.replace(' ', '_').replace('-', '_').replace('.', '_')
    # Remove any other invalid characters
    table_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in table_name)
    # Ensure it starts with a letter or underscore
    if table_name and not (table_name[0].isalpha() or table_name[0] == '_'):
        table_name = '_' + table_name
    return table_name

def create_bronze_table(con, table_name, columns):
    """Create a bronze table for a specific file - drops and recreates for fresh data"""
    logger.info(f"Creating bronze table: {table_name}")
    
    # Create bronze schema if it doesn't exist
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    
    # Drop existing table to ensure fresh data
    con.execute(f"DROP TABLE IF EXISTS bronze.{table_name}")
    logger.info(f"Dropped existing bronze.{table_name} table")
    
    # Make column names unique for SQL (handle duplicates within the same file)
    sql_columns = []
    used_names = set()  # Track all column names we've used (case-insensitive for DuckDB)
    
    for col in columns:
        original_col = col
        counter = 0
        unique_col = col
        
        # Keep trying until we find a unique name (case-insensitive check for DuckDB)
        while unique_col.lower() in [n.lower() for n in used_names]:
            counter += 1
            unique_col = f"{original_col}_{counter}"
        
        sql_columns.append(f'"{unique_col}" VARCHAR')
        used_names.add(unique_col)
        
        if counter > 0:
            logger.debug(f"Renamed duplicate column '{original_col}' to '{unique_col}'")
    
    # Add metadata columns
    sql_columns.extend([
        'line_number INTEGER',
        'extraction_timestamp VARCHAR',
        'file_name VARCHAR'
    ])
    
    create_table_sql = f"""
    CREATE TABLE bronze.{table_name} (
        {', '.join(sql_columns)}
    )
    """
    
    con.execute(create_table_sql)
    logger.info(f"Table bronze.{table_name} created successfully with {len(columns)} data columns")

def process_excel_file(file_path, con):
    """Process a single Excel file and load data to its own bronze table"""
    file_name = os.path.basename(file_path)
    table_name = sanitize_table_name(file_name)
    logger.info(f"Processing file: {file_name} -> table: {table_name}")
    
    try:
        # Check if FET sheet exists
        if not check_sheet_exists(file_path, SHEET_NAME):
            logger.warning(f"Sheet '{SHEET_NAME}' not found in {file_name}, skipping...")
            return 0
        
        logger.info(f"Reading sheet '{SHEET_NAME}' from {file_name} (header in row 2)")
        
        # Use optimized reading settings
        # header=1 means row 2 in Excel (0-indexed, so row 1 = header=0, row 2 = header=1)
        df = pd.read_excel(
            file_path, 
            sheet_name=SHEET_NAME,
            engine='openpyxl',
            header=1,  # Column names are in row 2
            dtype=str  # Read all as strings to avoid type issues
        )
        
        logger.info(f"Read {len(df)} rows from {file_name}")
        
        if len(df) == 0:
            logger.warning(f"No data found in {file_name}")
            return 0
        
        # Get original column names (preserve order from sheet)
        original_columns = df.columns.tolist()
        
        # Create bronze table for this file with its columns
        create_bronze_table(con, table_name, original_columns)
        
        # Make column names unique for SQL (handle duplicates within this file)
        used_names = set()
        column_mapping = {}
        unique_columns = []
        
        for col in original_columns:
            original_col = col
            counter = 0
            unique_col = col
            
            # Keep trying until we find a unique name (case-insensitive check for DuckDB)
            while unique_col.lower() in [n.lower() for n in used_names]:
                counter += 1
                unique_col = f"{original_col}_{counter}"
            
            column_mapping[col] = unique_col
            unique_columns.append(unique_col)
            used_names.add(unique_col)
        
        # Rename columns to unique names for SQL compatibility
        df = df.rename(columns=column_mapping)
        
        # Reorder columns to match unique_columns order
        df = df[unique_columns]
        
        # Clean data - replace empty strings with None
        for col in df.columns:
            df[col] = df[col].replace('', None)
        
        # Add metadata columns
        extraction_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create new DataFrame with all columns to avoid fragmentation
        new_df = df.copy()
        new_df['line_number'] = new_df.index  # Add line number based on DataFrame index
        new_df['extraction_timestamp'] = extraction_timestamp
        new_df['file_name'] = file_name
        df = new_df
        
        logger.info(f"Inserting {len(df)} rows from {file_name} into bronze.{table_name}")
        
        # Use the same efficient strategy as mesclada - direct DataFrame insertion
        # All data is already strings from dtype=str
        con.execute(f"INSERT INTO bronze.{table_name} SELECT * FROM df")
        
        logger.info(f"Successfully inserted {len(df)} rows from {file_name} into bronze.{table_name}")
        return len(df)
        
    except Exception as e:
        logger.error(f"Error processing file {file_name}: {e}")
        raise

def main():
    """Main function to load all planilha_embriologia Excel files to bronze.planilha_embriologia"""
    logger.info("Starting Planilha Embriologia data loader")
    logger.info(f"Reading from sheet: {SHEET_NAME}")
    logger.info(f"DuckDB path: {DUCKDB_PATH}")
    logger.info(f"Data input directory: {DATA_INPUT_DIR}")
    
    try:
        # Create DuckDB connection
        logger.info("Creating DuckDB connection...")
        con = get_duckdb_connection()
        logger.info("DuckDB connection created successfully")
        
        # Get all Excel files from year subfolders
        excel_files = get_all_excel_files()
        
        # Process each file - each file gets its own table
        total_rows = 0
        files_processed = 0
        table_names = []
        
        for file_path in excel_files:
            try:
                rows = process_excel_file(file_path, con)
                total_rows += rows
                files_processed += 1
                table_name = sanitize_table_name(os.path.basename(file_path))
                table_names.append(table_name)
            except Exception as e:
                logger.error(f"Failed to process {os.path.basename(file_path)}: {e}")
                # Continue with next file instead of stopping
                continue
        
        # Get final statistics for all tables
        total_rows_all_tables = 0
        for table_name in table_names:
            try:
                result = con.execute(f'SELECT COUNT(*) FROM bronze.{table_name}').fetchone()
                count = result[0] if result else 0
                total_rows_all_tables += count
                logger.info(f"  bronze.{table_name}: {count:,} rows")
            except Exception as e:
                logger.warning(f"Could not get count for bronze.{table_name}: {e}")
        
        # Final summary
        logger.info("=" * 50)
        logger.info("LOADING SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Sheet name: {SHEET_NAME}")
        logger.info(f"Files found: {len(excel_files)}")
        logger.info(f"Files processed successfully: {files_processed}")
        logger.info(f"Total rows loaded to bronze: {total_rows:,}")
        logger.info(f"Total rows across all tables: {total_rows_all_tables:,}")
        logger.info(f"Tables created: {len(table_names)}")
        logger.info("=" * 50)
        
        # Close connection
        con.close()
        logger.info("Planilha Embriologia data loader completed")
        
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        raise

if __name__ == "__main__":
    main()

