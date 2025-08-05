#!/usr/bin/env python3
"""
Database Copy Loader - Direct copy from MySQL to DuckDB
Preserves original data types and structure without transformations.
"""

import logging
import yaml
import pandas as pd
import duckdb
from sqlalchemy import create_engine, text
from datetime import datetime
import hashlib
import os

# Load config and logging level
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'params.yml')
with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)
logging_level_str = config.get('logging_level', 'INFO').upper()
logging_level = getattr(logging, logging_level_str, logging.INFO)

# Setup logging
LOGS_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
script_name = os.path.splitext(os.path.basename(__file__))[0]
LOG_PATH = os.path.join(LOGS_DIR, f'{script_name}_{timestamp}.log')
logging.basicConfig(
    level=logging_level,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logger.info(f'Loaded logging level: {logging_level_str}')

def load_config():
    """Load configuration from params.yml"""
    # Config is already loaded at module level
    return config

def get_mysql_connection(config):
    """Create MySQL connection using SQLAlchemy"""
    connection_string = config['db']['connection_string']
    return create_engine(connection_string)

def get_duckdb_connection(duckdb_path):
    """Create DuckDB connection"""
    return duckdb.connect(duckdb_path)

def get_table_schema(engine, table_name):
    """Get the schema (column names and types) of a MySQL table"""
    with engine.connect() as conn:
        # Get column information
        result = conn.execute(text(f"DESCRIBE {table_name}"))
        columns = result.fetchall()
        
        schema = {}
        for col in columns:
            col_name = col[0]
            col_type = col[1]
            col_null = col[2]
            col_key = col[3]
            col_default = col[4]
            col_extra = col[5]
            
            schema[col_name] = {
                'type': col_type,
                'null': col_null,
                'key': col_key,
                'default': col_default,
                'extra': col_extra
            }
        
        return schema

def map_mysql_to_duckdb_type(mysql_type):
    """Map MySQL data types to DuckDB data types - Conservative approach"""
    mysql_type = mysql_type.upper()
    
    # For now, let's be conservative and use VARCHAR for most types
    # This avoids type conversion issues and preserves data integrity
    
    # Only use specific types for clearly defined cases
    if 'DATE' in mysql_type:
        return 'DATE'
    elif 'DATETIME' in mysql_type:
        return 'TIMESTAMP'
    elif 'TIMESTAMP' in mysql_type:
        return 'TIMESTAMP'
    elif 'TIME' in mysql_type:
        return 'TIME'
    
    # For all other types, use VARCHAR to avoid conversion issues
    # This includes INT, DOUBLE, FLOAT, etc. that might have empty strings
    return 'VARCHAR'

def create_duckdb_table(con, table_name, schema):
    """Create DuckDB table with the same structure as MySQL (only if it doesn't exist)"""
    # Check if table exists
    table_exists = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_schema = 'bronze' AND table_name = '{table_name}'
    """).fetchone()[0]
    
    if table_exists:
        logger.info(f"Table bronze.{table_name} already exists, skipping creation")
        return
    
    columns = []
    for col_name, col_info in schema.items():
        duckdb_type = map_mysql_to_duckdb_type(col_info['type'])
        # Make all columns nullable to avoid constraint issues
        columns.append(f"{col_name} {duckdb_type}")
    
    # Add our standard columns
    columns.extend([
        "hash VARCHAR NOT NULL",
        "extraction_timestamp VARCHAR NOT NULL"
    ])
    
    create_sql = f"""
    CREATE TABLE bronze.{table_name} (
        {', '.join(columns)}
    )
    """
    
    logger.info(f"Creating table bronze.{table_name}")
    con.execute(create_sql)

def copy_table_data(engine, con, table_name, config):
    """Copy data from MySQL table to DuckDB table (incremental)"""
    logger.info(f"Copying data from MySQL table {table_name} (incremental mode)")
    
    # Get the query from config
    query = config['db']['tables'][table_name]['query']
    
    # Read data from MySQL
    with engine.connect() as mysql_conn:
        df = pd.read_sql(query, mysql_conn)
    
    logger.info(f"Read {len(df)} rows from MySQL table {table_name}")
    
    if len(df) == 0:
        logger.warning(f"No data found in table {table_name}")
        return
    
    # Clean empty strings that might cause type conversion issues
    for col in df.columns:
        if df[col].dtype == 'object':
            # Replace empty strings with None for better type handling
            df[col] = df[col].replace('', None)
    
    # Add hash and extraction_timestamp columns
    extraction_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create hash for each row (more efficient approach)
    hash_values = df.apply(lambda row: hashlib.md5(
        str(row.values).encode('utf-8')
    ).hexdigest(), axis=1)
    
    # Create new DataFrame with all columns to avoid fragmentation
    new_df = df.copy()
    new_df['hash'] = hash_values
    new_df['extraction_timestamp'] = extraction_timestamp
    df = new_df
    
    # Get existing hashes from DuckDB to avoid duplicates
    try:
        existing_hashes = set(con.execute(f"SELECT hash FROM bronze.{table_name}").fetchall())
        existing_hashes = {row[0] for row in existing_hashes if row[0]}
        logger.info(f"Found {len(existing_hashes)} existing hashes in bronze.{table_name}")
    except Exception as e:
        logger.warning(f"Could not get existing hashes for {table_name}: {e}")
        existing_hashes = set()
    
    # Filter out rows that already exist (based on hash)
    new_rows = df[~df['hash'].isin(list(existing_hashes))]
    
    if len(new_rows) == 0:
        logger.info(f"No new rows to insert for {table_name}")
        return
    
    logger.info(f"Inserting {len(new_rows)} new rows to bronze.{table_name}")
    
    # Insert only new rows
    con.execute(f"INSERT INTO bronze.{table_name} SELECT * FROM new_rows")
    
    logger.info(f"Successfully inserted {len(new_rows)} new rows to bronze.{table_name}")
    logger.info(f"Total rows in bronze.{table_name}: {con.execute(f'SELECT COUNT(*) FROM bronze.{table_name}').fetchone()[0]}")

def main():
    """Main function to copy database"""
    logger.info("Starting database copy loader")
    
    # Load configuration
    config = load_config()
    duckdb_path = config['duckdb_path']
    
    logger.info(f"DuckDB path: {duckdb_path}")
    
    # Create connections
    mysql_engine = get_mysql_connection(config)
    duckdb_con = get_duckdb_connection(duckdb_path)
    
    # Create bronze schema if it doesn't exist
    duckdb_con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    
    # Process each table
    tables = config['db']['tables'].keys()
    
    for table_name in tables:
        try:
            logger.info(f"Processing table {table_name}")
            
            # Get MySQL table schema
            schema = get_table_schema(mysql_engine, table_name)
            
            # Create DuckDB table with same structure
            create_duckdb_table(duckdb_con, table_name, schema)
            
            # Copy data
            copy_table_data(mysql_engine, duckdb_con, table_name, config)
            
            logger.info(f"Successfully processed table {table_name}")
            
        except Exception as e:
            logger.error(f"Error processing table {table_name}: {e}")
            continue
    
    # Close connections
    duckdb_con.close()
    logger.info("Database copy loader finished")

if __name__ == "__main__":
    main() 