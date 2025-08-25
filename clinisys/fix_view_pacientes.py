#!/usr/bin/env python3
"""
Fix view_pacientes table - Drop and recreate to handle new 'inativo' column
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

def drop_view_pacientes_table(con):
    """Drop the view_pacientes table if it exists"""
    table_exists = con.execute("""
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_schema = 'bronze' AND table_name = 'view_pacientes'
    """).fetchone()[0]
    
    if table_exists:
        logger.info("Dropping existing table bronze.view_pacientes")
        con.execute("DROP TABLE bronze.view_pacientes")
        logger.info("Successfully dropped bronze.view_pacientes")
    else:
        logger.info("Table bronze.view_pacientes does not exist, nothing to drop")

def create_view_pacientes_table(con, schema):
    """Create the view_pacientes table with the updated schema"""
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
    CREATE TABLE bronze.view_pacientes (
        {', '.join(columns)}
    )
    """
    
    logger.info("Creating table bronze.view_pacientes with updated schema")
    logger.info(f"Columns: {list(schema.keys())}")
    con.execute(create_sql)
    logger.info("Successfully created bronze.view_pacientes")

def copy_view_pacientes_data(engine, con, config):
    """Copy data from MySQL view_pacientes to DuckDB"""
    logger.info("Copying data from MySQL view_pacientes")
    
    # Get the query from config
    query = config['db']['tables']['view_pacientes']['query']
    
    # Read data from MySQL
    with engine.connect() as mysql_conn:
        df = pd.read_sql(query, mysql_conn)
    
    logger.info(f"Read {len(df)} rows from MySQL view_pacientes")
    
    if len(df) == 0:
        logger.warning("No data found in view_pacientes")
        return
    
    # Clean empty strings that might cause type conversion issues
    for col in df.columns:
        if df[col].dtype == 'object':
            # Replace empty strings with None for better type handling
            df[col] = df[col].replace('', None)
    
    # Add hash and extraction_timestamp columns
    extraction_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create hash for each row
    hash_values = df.apply(lambda row: hashlib.md5(
        str(row.values).encode('utf-8')
    ).hexdigest(), axis=1)
    
    # Create new DataFrame with all columns
    new_df = df.copy()
    new_df['hash'] = hash_values
    new_df['extraction_timestamp'] = extraction_timestamp
    df = new_df
    
    # Insert all rows (since we're recreating the table)
    logger.info(f"Inserting {len(df)} rows to bronze.view_pacientes")
    con.execute("INSERT INTO bronze.view_pacientes SELECT * FROM df")
    
    logger.info(f"Successfully inserted {len(df)} rows to bronze.view_pacientes")
    logger.info(f"Total rows in bronze.view_pacientes: {con.execute('SELECT COUNT(*) FROM bronze.view_pacientes').fetchone()[0]}")

def main():
    """Main function to fix view_pacientes table"""
    logger.info("Starting view_pacientes table fix")
    
    # Load configuration
    duckdb_path = config['duckdb_path']
    
    logger.info(f"DuckDB path: {duckdb_path}")
    
    # Create connections
    mysql_engine = get_mysql_connection(config)
    duckdb_con = get_duckdb_connection(duckdb_path)
    
    # Create bronze schema if it doesn't exist
    duckdb_con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    
    try:
        # Get MySQL table schema
        logger.info("Getting MySQL view_pacientes schema")
        schema = get_table_schema(mysql_engine, 'view_pacientes')
        logger.info(f"MySQL schema has {len(schema)} columns: {list(schema.keys())}")
        
        # Drop existing table
        drop_view_pacientes_table(duckdb_con)
        
        # Create new table with updated schema
        create_view_pacientes_table(duckdb_con, schema)
        
        # Copy data
        copy_view_pacientes_data(mysql_engine, duckdb_con, config)
        
        logger.info("Successfully fixed view_pacientes table")
        
    except Exception as e:
        logger.error(f"Error fixing view_pacientes table: {e}")
        raise
    
    finally:
        # Close connections
        duckdb_con.close()
        logger.info("View_pacientes table fix finished")

if __name__ == "__main__":
    main()
