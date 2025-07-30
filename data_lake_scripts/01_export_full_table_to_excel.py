#!/usr/bin/env python3
"""
Script to export embryoscope_clinisys_combined table from DuckDB to Excel format.

This script reads the combined embryoscope and clinisys data from the gold layer
and exports it to an Excel file in the data_exports directory.
"""

import duckdb
import pandas as pd
import os
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/export_embryoscope_clinisys_combined_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)

def export_embryoscope_clinisys_combined():
    """
    Export the embryoscope_clinisys_combined table to Excel format.
    """
    try:
        # Database path
        db_path = '../database/huntington_data_lake.duckdb'
        
        # Check if database exists
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database file not found: {db_path}")
        
        logging.info(f"Connecting to database: {db_path}")
        
        # Connect to database
        con = duckdb.connect(db_path)
        
        # Check if table exists
        table_exists = con.execute("""
            SELECT COUNT(*) as count 
            FROM information_schema.tables 
            WHERE table_schema = 'gold' 
            AND table_name = 'embryoscope_clinisys_combined'
        """).fetchone()[0]
        
        if table_exists == 0:
            raise ValueError("Table 'gold.embryoscope_clinisys_combined' not found in database")
        
        logging.info("Table found. Getting table information...")
        
        # Get table schema
        schema = con.execute("DESCRIBE gold.embryoscope_clinisys_combined").fetchdf()
        logging.info(f"Table has {len(schema)} columns")
        
        # Get row count
        row_count = con.execute("SELECT COUNT(*) as count FROM gold.embryoscope_clinisys_combined").fetchone()[0]
        logging.info(f"Table has {row_count:,} rows")
        
        # Create output directory if it doesn't exist
        output_dir = 'data_exports'
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate output filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(output_dir, f'embryoscope_clinisys_combined_{timestamp}.xlsx')
        
        logging.info(f"Starting data export to: {output_file}")
        
        # Read data in chunks to handle large datasets efficiently
        chunk_size = 50000  # Adjust based on available memory
        total_chunks = (row_count + chunk_size - 1) // chunk_size
        
        logging.info(f"Exporting data in {total_chunks} chunks of {chunk_size} rows each")
        
        # Create Excel writer with xlsxwriter engine for better performance
        with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
            
            # Write schema information to first sheet
            schema.to_excel(writer, sheet_name='Table_Schema', index=False)
            logging.info("Schema information written to 'Table_Schema' sheet")
            
            # Write data in chunks
            for chunk_num in range(total_chunks):
                offset = chunk_num * chunk_size
                
                logging.info(f"Processing chunk {chunk_num + 1}/{total_chunks} (rows {offset + 1}-{min(offset + chunk_size, row_count)})")
                
                # Query data with limit and offset
                query = f"""
                    SELECT * FROM gold.embryoscope_clinisys_combined 
                    LIMIT {chunk_size} OFFSET {offset}
                """
                
                chunk_df = con.execute(query).fetchdf()
                
                if chunk_df.empty:
                    logging.info("No more data to process")
                    break
                
                # Write chunk to Excel
                sheet_name = f'Data_Chunk_{chunk_num + 1:03d}'
                chunk_df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                logging.info(f"Chunk {chunk_num + 1} written to sheet '{sheet_name}' ({len(chunk_df)} rows)")
            
            # Create a summary sheet
            summary_data = {
                'Metric': [
                    'Total Rows',
                    'Total Columns', 
                    'Export Date',
                    'Database Path',
                    'Table Name',
                    'Chunks Exported'
                ],
                'Value': [
                    row_count,
                    len(schema),
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    db_path,
                    'gold.embryoscope_clinisys_combined',
                    total_chunks
                ]
            }
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Export_Summary', index=False)
            logging.info("Summary information written to 'Export_Summary' sheet")
        
        # Close database connection
        con.close()
        
        # Verify file was created and get file size
        if os.path.exists(output_file):
            file_size = os.path.getsize(output_file) / (1024 * 1024)  # Size in MB
            logging.info(f"Export completed successfully!")
            logging.info(f"Output file: {output_file}")
            logging.info(f"File size: {file_size:.2f} MB")
            
            return output_file
        else:
            raise FileNotFoundError("Output file was not created")
            
    except Exception as e:
        logging.error(f"Error during export: {str(e)}")
        raise

def main():
    """
    Main function to run the export process.
    """
    try:
        logging.info("Starting embryoscope_clinisys_combined export process")
        
        output_file = export_embryoscope_clinisys_combined()
        
        logging.info("Export process completed successfully!")
        print(f"\n✅ Export completed successfully!")
        print(f"📁 Output file: {output_file}")
        
    except Exception as e:
        logging.error(f"Export process failed: {str(e)}")
        print(f"\n❌ Export process failed: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main()) 