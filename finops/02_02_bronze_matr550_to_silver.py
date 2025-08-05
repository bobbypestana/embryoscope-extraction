#!/usr/bin/env python3
"""
FinOps Bronze to Silver Transformation
Consolidates all FinOps billing tables from bronze to silver with standardization and enrichment.
"""

import logging
import pandas as pd
import duckdb
from datetime import datetime
import hashlib
import os
import re
from typing import List, Dict, Any

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
BRONZE_PATTERN = '0401_FAT-x%'  # Pattern to identify FinOps tables
SILVER_TABLE = 'finops_billing_silver'

# Import column mapping
from column_mapping import COLUMN_MAPPING, EXCLUDED_COLUMNS, STANDARDIZED_COLUMNS, COLUMN_TYPES

# Clinic code mapping (extracted from table names)
CLINIC_CODES = {
    'xBH': 'Belo Horizonte',
    'xBR': 'Brasília',
    'xCA': 'Campinas',
    'xIB': 'Ibirapuera',
    'xIB2': 'Ibirapuera 2',
    'xSA': 'Salvador',
    'xSJ': 'Santa Joana',
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

def get_finops_bronze_tables(con) -> List[str]:
    """Get all FinOps bronze tables using pattern matching"""
    try:
        # Query to find all tables matching the FinOps pattern
        query = f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'bronze' 
        AND table_name LIKE '{BRONZE_PATTERN}'
        ORDER BY table_name
        """
        result = con.execute(query).fetchall()
        tables = [row[0] for row in result]
        logger.info(f"Found {len(tables)} FinOps bronze tables: {tables}")
        return tables
    except Exception as e:
        logger.error(f"Error getting FinOps bronze tables: {e}")
        return []

def extract_clinic_info(table_name: str) -> Dict[str, str]:
    """Extract clinic code and name from table name"""
    # Extract clinic code from table name (e.g., '0401_FAT-xBH' -> 'xBH')
    match = re.search(r'0401_FAT-(x[A-Z0-9]+)', table_name)
    if match:
        clinic_code = match.group(1)
        clinic_name = CLINIC_CODES.get(clinic_code, f"Unknown ({clinic_code})")
        return {
            'clinic_code': clinic_code,
            'clinic_name': clinic_name
        }
    else:
        logger.warning(f"Could not extract clinic info from table name: {table_name}")
        return {
            'clinic_code': 'UNKNOWN',
            'clinic_name': 'Unknown Clinic'
        }

def clean_column_name(col_name: str) -> str:
    """Clean column name for SQL compatibility"""
    if pd.isna(col_name):
        return 'unnamed_column'
    
    # Convert to string and clean
    clean_name = str(col_name).strip()
    
    # Replace problematic characters
    clean_name = clean_name.replace(' ', '_')
    clean_name = clean_name.replace('-', '_')
    clean_name = clean_name.replace('(', '')
    clean_name = clean_name.replace(')', '')
    clean_name = clean_name.replace('%', 'pct')
    clean_name = clean_name.replace('.', '_')
    clean_name = clean_name.replace(',', '_')
    
    # Remove multiple underscores
    clean_name = re.sub(r'_+', '_', clean_name)
    
    # Ensure it starts with a letter
    if clean_name and not clean_name[0].isalpha():
        clean_name = 'col_' + clean_name
    
    return clean_name.lower()

def parse_date_safe(date_str, default=None):
    """Safely parse date string"""
    if pd.isna(date_str) or date_str == '' or date_str is None:
        return default
    
    try:
        # Try different date formats
        date_str = str(date_str).strip()
        
        # Common Brazilian date formats
        formats = [
            '%d/%m/%Y', '%d/%m/%y', '%Y-%m-%d', '%d-%m-%Y',
            '%d.%m.%Y', '%Y/%m/%d', '%d/%m/%Y %H:%M:%S'
        ]
        
        for fmt in formats:
            try:
                return pd.to_datetime(date_str, format=fmt).date()
            except:
                continue
        
        # Try pandas automatic parsing
        return pd.to_datetime(date_str).date()
    except:
        return default

def parse_decimal_safe(value, default=0.0):
    """Safely parse decimal value"""
    if pd.isna(value) or value == '' or value is None:
        return default
    
    try:
        # Convert to string and clean
        value_str = str(value).strip()
        
        # Handle edge cases like ",   ."
        if value_str in [',', '.', ',   .', '   .', '   ,', '   ', '']:
            return default
        
        # Remove currency symbols and spaces
        value_str = re.sub(r'[R$\s]', '', value_str)
        
        # Handle different decimal separators
        value_str = value_str.replace(',', '.')
        
        # Remove any remaining non-numeric characters except decimal point and minus
        value_str = re.sub(r'[^\d.-]', '', value_str)
        
        # Handle empty string after cleaning
        if not value_str or value_str == '.' or value_str == '-':
            return default
        
        # Parse as float
        return float(value_str)
    except:
        return default

def standardize_dataframe(df: pd.DataFrame, clinic_info: Dict[str, str]) -> pd.DataFrame:
    """Standardize and clean DataFrame using column mapping"""
    logger.info(f"Standardizing DataFrame with {len(df)} rows")
    
    # Create a copy to avoid modifying original
    df_clean = df.copy()
    
    # Apply column mapping - only keep columns we need
    new_columns = {}
    for col in df_clean.columns:
        if col in EXCLUDED_COLUMNS:
            continue  # Skip excluded columns silently
        
        if col in COLUMN_MAPPING:
            new_columns[col] = COLUMN_MAPPING[col]
        else:
            # Clean column name for unmapped columns
            new_columns[col] = clean_column_name(col)
    
    # Rename columns
    df_clean = df_clean.rename(columns=new_columns)
    
    # Add clinic information
    df_clean['clinic_code'] = clinic_info['clinic_code']
    df_clean['clinic_name'] = clinic_info['clinic_name']
    
    # Vectorized data type conversions for much better performance
    
    # Standardize date columns - use pandas vectorized operations
    if 'data_emissao' in df_clean.columns:
        df_clean['data_emissao'] = pd.to_datetime(df_clean['data_emissao'], errors='coerce').dt.date
    
    # Standardize monetary columns - use pandas vectorized operations
    monetary_columns = [col for col in df_clean.columns if 'valor' in col.lower()]
    for col in monetary_columns:
        if col in df_clean.columns:
            # Convert to string, clean, and parse as float
            df_clean[col] = (df_clean[col].astype(str)
                           .str.replace(r'[R$\s]', '', regex=True)
                           .str.replace(',', '.')
                           .str.replace(r'[^\d.-]', '', regex=True)
                           .replace(['', '.', '-', '..', '...'], 0.0)  # Handle multiple dots
                           .astype(float, errors='ignore')
                           .fillna(0.0))
    
    # Standardize comissao column with vectorized operations
    if 'comissao' in df_clean.columns:
        df_clean['comissao'] = (df_clean['comissao'].astype(str)
                               .str.replace(r'[R$\s]', '', regex=True)
                               .str.replace(',', '.')
                               .str.replace(r'[^\d.-]', '', regex=True)
                               .replace(['', '.', '-', '..', '...'], 0.0)  # Handle multiple dots
                               .astype(float, errors='ignore')
                               .fillna(0.0))
    
    # Only add clinic information - no other enrichment columns
    
    # Ensure all expected columns exist (fill with None if missing)
    for col in STANDARDIZED_COLUMNS:
        if col not in df_clean.columns:
            df_clean[col] = None
    
    # Reorder columns to match the standardized schema
    df_clean = df_clean.reindex(columns=STANDARDIZED_COLUMNS)
    
    logger.info(f"Standardization completed. Final shape: {df_clean.shape}")
    return df_clean

def get_standardized_columns() -> List[str]:
    """Get the standardized column list"""
    logger.info(f"Using standardized schema with {len(STANDARDIZED_COLUMNS)} columns")
    return STANDARDIZED_COLUMNS

def create_silver_table(con, all_columns: List[str]):
    """Create silver table with standardized schema"""
    logger.info(f"Creating silver table: {SILVER_TABLE}")
    
    # Create silver schema if it doesn't exist
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    
    # Drop existing table if it exists
    try:
        con.execute(f'DROP TABLE IF EXISTS silver."{SILVER_TABLE}"')
        logger.info(f"Dropped existing table silver.{SILVER_TABLE}")
    except Exception as e:
        logger.warning(f"Could not drop existing table: {e}")
    
    # Generate CREATE TABLE statement based on standardized columns
    columns = []
    for col in all_columns:
        if col in COLUMN_TYPES:
            columns.append(f'"{col}" {COLUMN_TYPES[col]}')
        else:
            columns.append(f'"{col}" VARCHAR')
    
    create_table_sql = f"""
    CREATE TABLE silver.{SILVER_TABLE} (
        {', '.join(columns)}
    )
    """
    
    con.execute(create_table_sql)
    logger.info(f"Table silver.{SILVER_TABLE} created with {len(columns)} columns")

def process_bronze_table(con, table_name: str) -> int:
    """Process a single bronze table and load to silver"""
    logger.info(f"Processing bronze table: {table_name}")
    
    try:
        # Extract clinic information
        clinic_info = extract_clinic_info(table_name)
        logger.info(f"Clinic: {clinic_info['clinic_name']} ({clinic_info['clinic_code']})")
        
        # Read all columns and filter in memory - faster for large tables
        query = f'SELECT * FROM bronze."{table_name}"'
        df = con.execute(query).df()
        
        # Filter out excluded columns in memory
        columns_to_keep = [col for col in df.columns if col not in EXCLUDED_COLUMNS]
        df = df[columns_to_keep]
        
        # Log column count
        original_columns = len(df.columns) + len(EXCLUDED_COLUMNS.intersection(set(df.columns)))
        actual_columns = len(df.columns)
        logger.info(f"Read {len(df)} rows from {table_name} (original: {original_columns}, filtered: {actual_columns})")
        
        if len(df) == 0:
            logger.warning(f"No data found in {table_name}")
            return 0
        
        # Standardize data
        df_clean = standardize_dataframe(df, clinic_info)
        
        # Log final column count
        final_columns = len(df_clean.columns)
        logger.info(f"Transformation: {original_columns} -> {final_columns} columns")
        
        # Insert into silver table using DuckDB's efficient bulk insert
        logger.info(f"Inserting {len(df_clean)} rows into silver.{SILVER_TABLE}")
        
        # Use DuckDB's native DataFrame insertion - much faster
        con.execute(f'INSERT INTO silver."{SILVER_TABLE}" SELECT * FROM df_clean')
        
        logger.info(f"Successfully processed {table_name}")
        return len(df_clean)
        
    except Exception as e:
        logger.error(f"Error processing table {table_name}: {e}")
        return 0

def main():
    """Main function to transform bronze to silver"""
    logger.info("Starting FinOps bronze to silver transformation")
    logger.info(f"DuckDB path: {DUCKDB_PATH}")
    
    try:
        # Create DuckDB connection
        con = get_duckdb_connection()
        
        # Get all FinOps bronze tables
        bronze_tables = get_finops_bronze_tables(con)
        
        if not bronze_tables:
            logger.error("No FinOps bronze tables found")
            return
        
        # Get standardized columns
        all_columns = get_standardized_columns()
        
        # Create silver table with standardized schema
        create_silver_table(con, all_columns)
        
        # Process each bronze table
        total_rows = 0
        processed_tables = 0
        
        for table_name in bronze_tables:
            try:
                rows_processed = process_bronze_table(con, table_name)
                total_rows += rows_processed
                processed_tables += 1
                
            except Exception as e:
                logger.error(f"Error processing {table_name}: {e}")
                continue
        
        # Final summary
        logger.info("=" * 50)
        logger.info("TRANSFORMATION SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Tables processed: {processed_tables}/{len(bronze_tables)}")
        logger.info(f"Total rows in silver: {total_rows}")
        
        # Get final count from silver table
        try:
            result = con.execute(f'SELECT COUNT(*) FROM silver."{SILVER_TABLE}"').fetchone()
            silver_count = result[0] if result else 0
            logger.info(f"Verified silver table count: {silver_count}")
        except Exception as e:
            logger.warning(f"Could not verify silver table count: {e}")
        
        logger.info("=" * 50)
        
        # Close connection
        con.close()
        logger.info("FinOps bronze to silver transformation completed")
        
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        raise

if __name__ == "__main__":
    main() 