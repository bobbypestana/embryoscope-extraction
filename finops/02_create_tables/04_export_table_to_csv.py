#!/usr/bin/env python3
"""
Script to export tables from DuckDB database to CSV files.
Usage: python export_table_to_csv.py <schema> <table_name> [output_filename]
"""

import sys
import os
import duckdb
import pandas as pd
from datetime import datetime
import argparse
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/export_table_to_csv_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_database_path():
    """Get the path to the DuckDB database."""
    # Go up two levels from 02_create_tables to get to the root, then to database
    current_dir = os.path.dirname(os.path.abspath(__file__))
    finops_dir = os.path.dirname(current_dir)
    root_dir = os.path.dirname(finops_dir)
    db_path = os.path.join(root_dir, 'database', 'huntington_data_lake.duckdb')
    return db_path

def list_available_tables():
    """List all available schemas and tables in the database."""
    db_path = get_database_path()
    
    if not os.path.exists(db_path):
        logger.error(f"Database file not found: {db_path}")
        return
    
    try:
        with duckdb.connect(db_path, read_only=True) as conn:
            # Get all schemas
            schemas = conn.execute("SELECT schema_name FROM information_schema.schemata").fetchall()
            
            logger.info("Available schemas and tables:")
            for schema in schemas:
                schema_name = schema[0]
                logger.info(f"\nSchema: {schema_name}")
                
                # Get tables in this schema
                tables = conn.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = ?", 
                    [schema_name]
                ).fetchall()
                
                for table in tables:
                    table_name = table[0]
                    # Get row count
                    try:
                        count = conn.execute(f"SELECT COUNT(*) FROM \"{schema_name}\".\"{table_name}\"").fetchone()[0]
                        logger.info(f"  - {table_name} ({count:,} rows)")
                    except Exception as e:
                        logger.info(f"  - {table_name} (error getting count: {e})")
                        
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")

def export_table_to_csv(schema, table_name, output_filename=None):
    """
    Export a table from DuckDB to CSV.
    
    Args:
        schema (str): Database schema name
        table_name (str): Table name
        output_filename (str, optional): Output filename. If None, will use schema_table_timestamp.csv
    """
    db_path = get_database_path()
    
    if not os.path.exists(db_path):
        logger.error(f"Database file not found: {db_path}")
        return False
    
    # Create output directory if it doesn't exist
    # Go up one level from 02_create_tables to finops, then to data_export
    current_dir = os.path.dirname(os.path.abspath(__file__))
    finops_dir = os.path.dirname(current_dir)
    output_dir = os.path.join(finops_dir, 'data_export')
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate output filename if not provided
    if output_filename is None:
        output_filename = f"{schema}_{table_name}.csv"
    
    # Ensure .csv extension
    if not output_filename.endswith('.csv'):
        output_filename += '.csv'
    
    output_path = os.path.join(output_dir, output_filename)
    
    try:
        logger.info(f"Connecting to database: {db_path}")
        with duckdb.connect(db_path, read_only=True) as conn:
            # Check if table exists
            table_exists = conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
                [schema, table_name]
            ).fetchone()[0]
            
            if table_exists == 0:
                logger.error(f"Table {schema}.{table_name} does not exist!")
                return False
            
            # Get table info
            logger.info(f"Exporting table: {schema}.{table_name}")
            
            # Get row count
            count = conn.execute(f"SELECT COUNT(*) FROM \"{schema}\".\"{table_name}\"").fetchone()[0]
            logger.info(f"Table has {count:,} rows")
            
            # Export to CSV
            logger.info(f"Starting export to: {output_path}")
            
            # Use pandas to export with European CSV format: decimal separator as comma, field separator as semicolon, quote all
            df = conn.execute(f"SELECT * FROM \"{schema}\".\"{table_name}\"").df()
            df.to_csv(output_path, sep=';', decimal=',', quoting=1, index=False)  # quoting=1 means QUOTE_ALL
            
            # Verify the file was created and has content
            if os.path.exists(output_path):
                file_size = os.path.getsize(output_path)
                logger.info(f"Export completed successfully!")
                logger.info(f"Output file: {output_path}")
                logger.info(f"File size: {file_size:,} bytes")
                
                # Show first few lines as preview
                try:
                    df_preview = pd.read_csv(output_path, nrows=5)
                    logger.info(f"Preview of exported data:")
                    logger.info(f"\n{df_preview.to_string()}")
                except Exception as e:
                    logger.warning(f"Could not preview data: {e}")
                
                return True
            else:
                logger.error("Export failed - output file was not created")
                return False
                
    except Exception as e:
        logger.error(f"Error during export: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Export tables from DuckDB to CSV')
    parser.add_argument('--list', action='store_true', help='List all available schemas and tables')
    parser.add_argument('--schema', type=str, help='Schema name')
    parser.add_argument('--table', type=str, help='Table name')
    parser.add_argument('--output', type=str, help='Output filename (optional)')
    
    args = parser.parse_args()
    
    # Create logs directory if it doesn't exist
    logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    
    if args.list:
        list_available_tables()
        return
    
    if not args.schema or not args.table:
        logger.error("Please provide both schema and table name, or use --list to see available tables")
        parser.print_help()
        return
    
    success = export_table_to_csv(args.schema, args.table, args.output)
    
    if success:
        logger.info("Export completed successfully!")
    else:
        logger.error("Export failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
