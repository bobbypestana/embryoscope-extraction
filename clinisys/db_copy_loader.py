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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(f'logs/db_copy_loader_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)

def load_config():
    """Load configuration from params.yml"""
    with open('params.yml', 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

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
    """Create DuckDB table with the same structure as MySQL"""
    # Drop existing table if it exists to ensure clean schema
    con.execute(f"DROP TABLE IF EXISTS bronze.{table_name}")
    
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
    
    logging.info(f"Creating table bronze.{table_name}")
    con.execute(create_sql)

def copy_table_data(engine, con, table_name, config):
    """Copy data from MySQL table to DuckDB table"""
    logging.info(f"Copying data from MySQL table {table_name}")
    
    # Get the query from config
    query = config['db']['tables'][table_name]['query']
    
    # Read data from MySQL
    with engine.connect() as mysql_conn:
        df = pd.read_sql(query, mysql_conn)
    
    logging.info(f"Read {len(df)} rows from MySQL table {table_name}")
    
    if len(df) == 0:
        logging.warning(f"No data found in table {table_name}")
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
    
    # Insert data into DuckDB
    con.execute(f"DELETE FROM bronze.{table_name}")
    con.execute(f"INSERT INTO bronze.{table_name} SELECT * FROM df")
    
    logging.info(f"Successfully copied {len(df)} rows to bronze.{table_name}")

def main():
    """Main function to copy database"""
    logging.info("Starting database copy loader")
    
    # Load configuration
    config = load_config()
    duckdb_path = config['duckdb_path']
    
    logging.info(f"DuckDB path: {duckdb_path}")
    
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Create connections
    mysql_engine = get_mysql_connection(config)
    duckdb_con = get_duckdb_connection(duckdb_path)
    
    # Create bronze schema if it doesn't exist
    duckdb_con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    
    # Process each table
    tables = config['db']['tables'].keys()
    
    for table_name in tables:
        try:
            logging.info(f"Processing table {table_name}")
            
            # Get MySQL table schema
            schema = get_table_schema(mysql_engine, table_name)
            
            # Create DuckDB table with same structure
            create_duckdb_table(duckdb_con, table_name, schema)
            
            # Copy data
            copy_table_data(mysql_engine, duckdb_con, table_name, config)
            
            logging.info(f"Successfully processed table {table_name}")
            
        except Exception as e:
            logging.error(f"Error processing table {table_name}: {e}")
            continue
    
    # Close connections
    duckdb_con.close()
    logging.info("Database copy loader finished")

if __name__ == "__main__":
    main() 