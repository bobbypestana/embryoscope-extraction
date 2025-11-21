#!/usr/bin/env python3
"""
Export gold.planilha_embryoscope_combined table to Excel
Uses efficient methods for large tables:
- Reads data in chunks if table is very large
- Uses xlsxwriter for better memory efficiency
- Handles large column counts
"""

import duckdb as db
import pandas as pd
from datetime import datetime
import os
import logging

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

# Output directory (inside planilha_embriologia folder)
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data_output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_database_connection(read_only=True):
    """Create and return a connection to the huntington_data_lake database"""
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    path_to_db = os.path.join(repo_root, 'database', 'huntington_data_lake.duckdb')
    conn = db.connect(path_to_db, read_only=read_only)
    
    logger.info(f"Connected to database: {path_to_db} (read_only={read_only})")
    return conn

def get_table_info(conn):
    """Get table row count and column count"""
    row_count = conn.execute("SELECT COUNT(*) FROM gold.planilha_embryoscope_combined").fetchone()[0]
    col_info = conn.execute("DESCRIBE gold.planilha_embryoscope_combined").df()
    col_count = len(col_info)
    
    logger.info(f"Table info: {row_count:,} rows, {col_count} columns")
    return row_count, col_count

def export_to_excel_efficient(conn, output_path, chunk_size=100000, export_format='excel'):
    """
    Export table efficiently
    For very large files, CSV is much faster than Excel
    
    Args:
        conn: Database connection
        output_path: Output file path
        chunk_size: Chunk size for processing
        export_format: 'excel' or 'csv' (csv is much faster for large files)
    """
    logger.info(f"Starting export: {output_path}")
    
    # Get table info
    row_count, col_count = get_table_info(conn)
    
    # For very large files, recommend CSV
    if export_format == 'csv' or (row_count > 200000 and export_format == 'auto'):
        logger.info(f"Using CSV format (much faster for {row_count:,} rows)")
        csv_path = output_path.replace('.xlsx', '.csv')
        export_to_csv(conn, csv_path)
        logger.info(f"CSV export completed: {csv_path}")
        return csv_path
    else:
        # Check if we need chunking
        use_chunking = row_count > chunk_size
        
        if use_chunking:
            logger.info(f"Table is large ({row_count:,} rows). Using chunked export with chunk size: {chunk_size:,}")
            export_chunked(conn, output_path, chunk_size)
        else:
            logger.info(f"Table size is manageable. Exporting all data at once.")
            export_full(conn, output_path)
        
        logger.info(f"Export completed: {output_path}")
        
        # Verify file was created
        if os.path.exists(output_path):
            file_size = os.path.getsize(output_path) / (1024 * 1024)  # MB
            logger.info(f"Excel file created: {file_size:.2f} MB")
        else:
            logger.error(f"Excel file was not created at: {output_path}")
        
        return output_path

def export_to_csv(conn, output_path):
    """Fast CSV export with UTF-8-SIG encoding and forced quoting"""
    logger.info(f"Exporting to CSV: {output_path}")
    logger.info("Using UTF-8-SIG encoding and forced quoting for all fields")
    
    # Read data from database
    logger.info("Reading data from database...")
    df = conn.execute("SELECT * FROM gold.planilha_embryoscope_combined").df()
    logger.info(f"Read {len(df):,} rows, {len(df.columns)} columns")
    
    # Replace NaT values with None
    df = df.replace({pd.NaT: None})
    
    # Clean newline characters from ALL columns to avoid CSV parsing issues
    logger.info("Cleaning newline characters from data...")
    import re
    
    def clean_newlines(value):
        """Clean newline characters from a value"""
        if value is None or pd.isna(value):
            return None
        # Convert to string
        str_value = str(value)
        # Replace various newline characters with space
        str_value = str_value.replace('\r\n', ' ')
        str_value = str_value.replace('\n', ' ')
        str_value = str_value.replace('\r', ' ')
        # Replace multiple spaces with single space
        str_value = re.sub(r'  +', ' ', str_value)
        # Strip leading/trailing whitespace
        str_value = str_value.strip()
        # Return None if empty, otherwise return cleaned string
        return None if str_value == '' or str_value == 'nan' else str_value
    
    # Apply cleaning to all columns
    logger.info("Applying newline cleaning to all columns...")
    for col in df.columns:
        df[col] = df[col].apply(clean_newlines)
    
    # Write to CSV with UTF-8-SIG encoding and forced quoting
    logger.info("Writing to CSV with UTF-8-SIG encoding and forced quoting...")
    df.to_csv(
        output_path,
        index=False,
        encoding='utf-8-sig',  # UTF-8 with BOM for Excel compatibility
        quoting=1,  # QUOTE_ALL - force quoting for all fields
        quotechar='"',
        sep=',',
        lineterminator='\n'
    )
    
    csv_size = os.path.getsize(output_path) / (1024 * 1024)  # MB
    logger.info(f"CSV file created: {csv_size:.2f} MB")

def export_full(conn, output_path):
    """Export entire table at once (for smaller tables)"""
    logger.info("Reading all data from database...")
    
    # Read all data
    df = conn.execute("SELECT * FROM gold.planilha_embryoscope_combined").df()
    
    logger.info(f"Read {len(df):,} rows. Writing to Excel...")
    
    # Replace NaT values with None to avoid xlsxwriter errors
    df = df.replace({pd.NaT: None})
    
    # Write to Excel
    # Use xlsxwriter engine for better performance and memory efficiency
    with pd.ExcelWriter(
        output_path,
        engine='xlsxwriter'
    ) as writer:
        df.to_excel(writer, sheet_name='planilha_embryoscope_combined', index=False)
        
        # Get workbook and worksheet for formatting
        workbook = writer.book
        worksheet = writer.sheets['planilha_embryoscope_combined']
        
        # Auto-adjust column widths (optional, can be slow for many columns)
        if len(df.columns) <= 100:  # Only auto-adjust if reasonable number of columns
            logger.info("Auto-adjusting column widths...")
            for i, col in enumerate(df.columns):
                max_length = max(
                    df[col].astype(str).map(len).max(),
                    len(str(col))
                )
                # Limit max width to 50 characters
                adjusted_width = min(max_length + 2, 50)
                worksheet.set_column(i, i, adjusted_width)
        else:
            logger.info(f"Too many columns ({len(df.columns)}) to auto-adjust widths. Using default.")
    
    logger.info("Excel file written successfully")

def export_chunked(conn, output_path, chunk_size):
    """Export table using CSV intermediate (faster for large tables)
    For very large files, we offer two options:
    1. Fast CSV export (recommended - user can open in Excel)
    2. Excel export using openpyxl write-only mode (faster than xlsxwriter)
    """
    logger.info("Using CSV intermediate format for efficient export...")
    
    # Step 1: Export to CSV using DuckDB's fast COPY command
    csv_path = output_path.replace('.xlsx', '.csv')
    logger.info(f"Step 1: Exporting to CSV: {csv_path}")
    
    conn.execute(f"""
        COPY (SELECT * FROM gold.planilha_embryoscope_combined) 
        TO '{csv_path}' (HEADER, DELIMITER ',')
    """)
    
    csv_size = os.path.getsize(csv_path) / (1024 * 1024)  # MB
    logger.info(f"CSV file created: {csv_size:.2f} MB")
    
    # For very large files, Excel writing is extremely slow
    # Option 1: Just keep CSV (fastest, user can open in Excel)
    # Option 2: Convert to Excel using openpyxl write-only mode (faster)
    
    logger.info("Step 2: Converting CSV to Excel using openpyxl (write-only mode for speed)...")
    
    try:
        from openpyxl import Workbook
        from openpyxl.cell.cell import WriteOnlyCell
        from openpyxl.styles import Font
        
        # Create write-only workbook (much faster for large files)
        wb = Workbook(write_only=True)
        ws = wb.create_sheet('planilha_embryoscope_combined')
        
        # Read CSV in chunks and write to Excel
        logger.info("Reading CSV and writing to Excel in chunks...")
        chunk_num = 0
        total_rows = 0
        
        for chunk_df in pd.read_csv(csv_path, chunksize=chunk_size, low_memory=False):
            chunk_num += 1
            chunk_df = chunk_df.replace({pd.NaT: None})
            
            if chunk_num == 1:
                # First chunk: write header
                header_row = [str(col) for col in chunk_df.columns]
                ws.append(header_row)
                logger.info(f"Written header ({len(header_row)} columns)")
            
            # Write data rows
            for _, row in chunk_df.iterrows():
                # Convert row to list, handling NaN/NaT
                row_data = []
                for val in row:
                    if pd.isna(val):
                        row_data.append(None)
                    else:
                        row_data.append(val)
                ws.append(row_data)
            
            total_rows += len(chunk_df)
            logger.info(f"Processed chunk {chunk_num} ({len(chunk_df):,} rows, total: {total_rows:,})")
        
        # Save workbook
        logger.info(f"Saving Excel file with {total_rows:,} rows...")
        wb.save(output_path)
        logger.info("Excel file saved successfully")
        
    except ImportError:
        logger.warning("openpyxl not available, falling back to pandas to_excel (slower)")
        # Fallback to pandas method
        df = pd.read_csv(csv_path, low_memory=False)
        logger.info(f"Read {len(df):,} rows from CSV")
        df = df.replace({pd.NaT: None})
        
        logger.info("Writing to Excel (this may take a while for large files)...")
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='planilha_embryoscope_combined', index=False)
    
    # Optionally keep CSV file for faster access
    # Uncomment the next line if you want to keep the CSV file
    # logger.info(f"CSV file kept at: {csv_path}")
    
    # Or remove CSV file to save space
    if os.path.exists(csv_path):
        os.remove(csv_path)
        logger.info("Temporary CSV file removed")
    
    logger.info("Export completed successfully")

def main():
    """Main function to export table to Excel"""
    
    logger.info("=== EXPORTING PLANILHA_EMBRYOSCOPE_COMBINED TO EXCEL ===")
    logger.info(f"Timestamp: {datetime.now()}")
    logger.info("")
    
    try:
        # Connect to database
        logger.info("Connecting to database...")
        conn = get_database_connection(read_only=True)
        
        # Generate output filename with timestamp
        timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # For large files, CSV is much faster - user can open in Excel
        # Change 'csv' to 'excel' if you specifically need .xlsx format
        export_format = 'csv'  # Options: 'csv', 'excel', 'auto' (auto chooses based on size)
        
        if export_format == 'csv':
            output_filename = f'planilha_embryoscope_combined_{timestamp_str}.csv'
        else:
            output_filename = f'planilha_embryoscope_combined_{timestamp_str}.xlsx'
        
        output_path = os.path.join(OUTPUT_DIR, output_filename)
        
        # Export
        export_to_excel_efficient(conn, output_path, export_format=export_format)
        
        logger.info(f"\nSuccessfully exported table to: {output_path}")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()

