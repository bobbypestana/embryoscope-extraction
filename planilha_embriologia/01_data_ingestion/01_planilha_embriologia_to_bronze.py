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
import re

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
SHEETS_TO_LOAD = ['FET', 'FRESH']

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
        # Case insensitive check
        sheet_names_lower = [s.lower() for s in xl_file.sheet_names]
        return sheet_name.lower() in sheet_names_lower
    except Exception as e:
        logger.warning(f"Could not check sheets in {os.path.basename(file_path)}: {e}")
        return False

def generate_table_name(file_path, sheet_name):
    """Generate standardized table name: planilha_{year}_{location}_{sheet}"""
    try:
        # Extract year from parent folder name
        year = os.path.basename(os.path.dirname(file_path))
        
        # Extract location/name from filename
        # Filename example: "CASOS 2024 IBI.xlsx" -> "ibi"
        filename = os.path.basename(file_path)
        name_no_ext = os.path.splitext(filename)[0]
        
        # Clean up the name
        # Remove "CASOS", year, spaces, dashes
        clean_name = name_no_ext.lower().replace('casos', '').replace(year, '')
        clean_name = re.sub(r'[^a-z0-9]', '_', clean_name)
        clean_name = re.sub(r'_+', '_', clean_name).strip('_')
        
        # Construct table name
        table_name = f"planilha_{year}_{clean_name}_{sheet_name.lower()}"
        return table_name
    except Exception as e:
        logger.warning(f"Could not generate standard name, falling back to safe filename: {e}")
        # Fallback
        base = os.path.splitext(os.path.basename(file_path))[0]
        safe_base = re.sub(r'[^a-zA-Z0-9_]', '_', base).lower()
        return f"planilha_{safe_base}_{sheet_name.lower()}"

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
        'file_name VARCHAR',
        'sheet_name VARCHAR'
    ])
    
    create_table_sql = f"""
    CREATE TABLE bronze.{table_name} (
        {', '.join(sql_columns)}
    )
    """
    
    con.execute(create_table_sql)
    logger.info(f"Table bronze.{table_name} created successfully with {len(columns)} data columns")

def process_excel_file(file_path, con):
    """Process a single Excel file and load data from all configured sheets"""
    file_name = os.path.basename(file_path)
    total_loaded = 0
    
    for sheet in SHEETS_TO_LOAD:
        # Generate table name for this sheet
        table_name = generate_table_name(file_path, sheet)
        logger.info(f"Processing file: {file_name} [{sheet}] -> table: {table_name}")
        
        try:
            # Check if sheet exists
            if not check_sheet_exists(file_path, sheet):
                logger.warning(f"Sheet '{sheet}' not found in {file_name}, skipping...")
                continue
            
            logger.info(f"Reading sheet '{sheet}' from {file_name} (header in row 2)")
            
            # Use optimized reading settings
            df = pd.read_excel(
                file_path, 
                sheet_name=sheet,
                engine='openpyxl',
                header=1,  # Column names are in row 2
                dtype=str  # Read all as strings to avoid type issues
            )
            
            logger.info(f"Read {len(df)} rows from {file_name} [{sheet}]")
            
            if len(df) == 0:
                logger.warning(f"No data found in {file_name} [{sheet}]")
                continue
            
            # Get original column names (preserve order from sheet)
            original_columns = df.columns.tolist()
            
            # Create bronze table for this file/sheet
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
            new_df['sheet_name'] = sheet
            df = new_df
            
            logger.info(f"Inserting {len(df)} rows from {file_name} [{sheet}] into bronze.{table_name}")
            
            # Direct DataFrame insertion
            con.execute(f"INSERT INTO bronze.{table_name} SELECT * FROM df")
            
            logger.info(f"Successfully inserted {len(df)} rows")
            total_loaded += len(df)
            
        except Exception as e:
            logger.error(f"Error processing {file_name} [{sheet}]: {e}")
            # Continue with next sheet
            continue
            
    return total_loaded

def main():
    """Main function to load all planilha_embriologia Excel files to bronze tables"""
    logger.info("Starting Planilha Embriologia data loader")
    logger.info(f"Target sheets: {SHEETS_TO_LOAD}")
    logger.info(f"DuckDB path: {DUCKDB_PATH}")
    logger.info(f"Data input directory: {DATA_INPUT_DIR}")
    
    try:
        # Create DuckDB connection
        logger.info("Creating DuckDB connection...")
        con = get_duckdb_connection()
        logger.info("DuckDB connection created successfully")
        
        # Get all Excel files from year subfolders
        excel_files = get_all_excel_files()
        
        # Process each file
        total_rows = 0
        files_processed = 0
        
        # Import regex module since we used it in generate_table_name but forgot to import it at top level if it wasn't there
        import re 
        
        for file_path in excel_files:
            try:
                rows = process_excel_file(file_path, con)
                total_rows += rows
                files_processed += 1
            except Exception as e:
                logger.error(f"Failed to process {os.path.basename(file_path)}: {e}")
                continue
        
        # Final summary
        logger.info("=" * 50)
        logger.info("LOADING SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Files processed: {files_processed}/{len(excel_files)}")
        logger.info(f"Total rows loaded to bronze: {total_rows:,}")
        logger.info("=" * 50)
        
        # Close connection
        con.close()
        logger.info("Planilha Embriologia data loader completed")
        
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        raise

if __name__ == "__main__":
    main()

