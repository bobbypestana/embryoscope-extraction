#!/usr/bin/env python3
"""
Combine gold.embryoscope_clinisys_combined with silver.planilha_embriologia
Join conditions:
- micro_prontuario = PIN
- descong_em_DataTransferencia = DATA DA FET
"""

import duckdb as db
import pandas as pd
from datetime import datetime
import os
import logging
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

def get_database_connection(read_only=False):
    """Create and return a connection to the huntington_data_lake database"""
    # Resolve DB path relative to repository root
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    path_to_db = os.path.join(repo_root, 'database', 'huntington_data_lake.duckdb')
    conn = db.connect(path_to_db, read_only=read_only)
    
    logger.info(f"Connected to database: {path_to_db} (read_only={read_only})")
    return conn

def create_combined_table(conn):
    """Create the gold.planilha_embryoscope_combined table"""
    
    logger.info("Creating gold.planilha_embryoscope_combined table...")
    
    # Drop table if it exists
    conn.execute("DROP TABLE IF EXISTS gold.planilha_embryoscope_combined")
    logger.info("Dropped existing gold.planilha_embryoscope_combined table if it existed")
    
    # Get column lists from both tables to handle potential conflicts
    logger.info("Getting column lists from source tables...")
    embryoscope_cols = conn.execute("DESCRIBE gold.embryoscope_clinisys_combined").df()['column_name'].tolist()
    planilha_cols = conn.execute("DESCRIBE silver.planilha_embriologia").df()['column_name'].tolist()
    
    logger.info(f"Embryoscope columns: {len(embryoscope_cols)}")
    logger.info(f"Planilha columns: {len(planilha_cols)}")
    
    # Build SELECT clause with explicit column references
    # Use table aliases to avoid conflicts
    # Clean column names for aliases (replace spaces and special chars)
    def clean_column_alias(col_name):
        """Clean column name for use as alias (remove spaces, special chars)"""
        # Replace spaces and special characters with underscores
        cleaned = re.sub(r'[^a-zA-Z0-9_]', '_', col_name)
        # Remove multiple consecutive underscores
        cleaned = re.sub(r'_+', '_', cleaned)
        # Remove leading/trailing underscores
        cleaned = cleaned.strip('_')
        return cleaned
    
    select_columns = []
    for col in embryoscope_cols:
        # Remove embryoscope_ prefix, just use cleaned column name
        alias = clean_column_alias(col)
        select_columns.append(f'e."{col}" as "{alias}"')
    
    for col in planilha_cols:
        alias = f'planilha_{clean_column_alias(col)}'
        select_columns.append(f'p."{col}" as "{alias}"')
    
    # Create the combined table
    # Join on:
    # - micro_prontuario (embryoscope_clinisys_combined) = PIN (planilha_embriologia)
    # - descong_em_DataTransferencia (embryoscope_clinisys_combined) = DATA DA FET (planilha_embriologia)
    create_table_query = f"""
    CREATE TABLE gold.planilha_embryoscope_combined AS
    SELECT 
        {', '.join(select_columns)}
    FROM gold.embryoscope_clinisys_combined e
    FULL OUTER JOIN silver.planilha_embriologia p
        ON CAST(CAST(e.micro_prontuario AS INTEGER) AS VARCHAR) = CAST(CAST(p."PIN" AS INTEGER) AS VARCHAR)
        AND DATE(e.descong_em_DataTransferencia) = DATE(p."DATA DA FET")
    """
    
    logger.info("Executing join query...")
    logger.info("Join conditions:")
    logger.info("  - micro_prontuario = PIN")
    logger.info("  - descong_em_DataTransferencia = DATA DA FET")
    
    conn.execute(create_table_query)
    logger.info("Table gold.planilha_embryoscope_combined created successfully")
    
    # Get statistics
    # Column aliases: embryoscope columns have no prefix, planilha columns have planilha_ prefix
    stats = conn.execute("""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(CASE WHEN "micro_prontuario" IS NOT NULL THEN 1 END) as rows_with_embryoscope,
            COUNT(CASE WHEN "planilha_PIN" IS NOT NULL THEN 1 END) as rows_with_planilha,
            COUNT(CASE WHEN "micro_prontuario" IS NOT NULL AND "planilha_PIN" IS NOT NULL THEN 1 END) as matched_rows,
            COUNT(CASE WHEN "micro_prontuario" IS NOT NULL AND "planilha_PIN" IS NULL THEN 1 END) as embryoscope_only,
            COUNT(CASE WHEN "micro_prontuario" IS NULL AND "planilha_PIN" IS NOT NULL THEN 1 END) as planilha_only
        FROM gold.planilha_embryoscope_combined
    """).fetchdf()
    
    # Calculate matching rates
    total_rows = stats['total_rows'].iloc[0]
    rows_with_embryoscope = stats['rows_with_embryoscope'].iloc[0]
    rows_with_planilha = stats['rows_with_planilha'].iloc[0]
    matched_rows = stats['matched_rows'].iloc[0]
    embryoscope_only = stats['embryoscope_only'].iloc[0]
    planilha_only = stats['planilha_only'].iloc[0]
    
    # Calculate relative matching rates
    match_rate_embryoscope = (matched_rows / rows_with_embryoscope * 100) if rows_with_embryoscope > 0 else 0
    match_rate_planilha = (matched_rows / rows_with_planilha * 100) if rows_with_planilha > 0 else 0
    overall_match_rate = (matched_rows / total_rows * 100) if total_rows > 0 else 0
    
    logger.info("Table Statistics:")
    logger.info(f"   - Total rows: {total_rows:,}")
    logger.info(f"   - Rows with embryoscope data: {rows_with_embryoscope:,}")
    logger.info(f"   - Rows with planilha data: {rows_with_planilha:,}")
    logger.info(f"   - Matched rows (both sides): {matched_rows:,}")
    logger.info(f"   - Embryoscope only rows: {embryoscope_only:,}")
    logger.info(f"   - Planilha only rows: {planilha_only:,}")
    logger.info("")
    logger.info("Matching Rates:")
    logger.info(f"   - Match rate (from embryoscope perspective): {match_rate_embryoscope:.2f}% ({matched_rows:,}/{rows_with_embryoscope:,})")
    logger.info(f"   - Match rate (from planilha perspective): {match_rate_planilha:.2f}% ({matched_rows:,}/{rows_with_planilha:,})")
    logger.info(f"   - Overall match rate: {overall_match_rate:.2f}% ({matched_rows:,}/{total_rows:,})")
    
    return stats

def analyze_combined_data(conn):
    """Analyze the combined data"""
    
    logger.info("\nAnalyzing combined data...")
    
    # Sample data
    # Column aliases: embryoscope columns have no prefix, planilha columns have planilha_ prefix
    sample_data = conn.execute("""
        SELECT 
            "micro_prontuario",
            "planilha_PIN",
            "descong_em_DataTransferencia",
            "planilha_DATA_DA_FET",
            "oocito_id",
            "embryo_EmbryoID"
        FROM gold.planilha_embryoscope_combined
        WHERE "micro_prontuario" IS NOT NULL AND "planilha_PIN" IS NOT NULL
        LIMIT 10
    """).fetchdf()
    
    logger.info("\nSample Data (Top 10 matched rows):")
    logger.info(sample_data.to_string(index=False))
    
    # Check for unmatched rows
    unmatched_embryoscope = conn.execute("""
        SELECT COUNT(*) as count
        FROM gold.planilha_embryoscope_combined
        WHERE "micro_prontuario" IS NOT NULL AND "planilha_PIN" IS NULL
    """).fetchdf()
    
    unmatched_planilha = conn.execute("""
        SELECT COUNT(*) as count
        FROM gold.planilha_embryoscope_combined
        WHERE "micro_prontuario" IS NULL AND "planilha_PIN" IS NOT NULL
    """).fetchdf()
    
    logger.info(f"\nUnmatched rows:")
    logger.info(f"   - Embryoscope only: {unmatched_embryoscope['count'].iloc[0]:,}")
    logger.info(f"   - Planilha only: {unmatched_planilha['count'].iloc[0]:,}")

def main():
    """Main function to create the combined table"""
    
    logger.info("=== COMBINING EMBRYOSCOPE_CLINISYS_COMBINED WITH PLANILHA_EMBRIOLOGIA ===")
    logger.info(f"Timestamp: {datetime.now()}")
    logger.info("")
    
    try:
        # Connect to database
        logger.info("Connecting to database...")
        conn = get_database_connection()
        
        # Create the table
        logger.info("Creating combined table...")
        table_stats = create_combined_table(conn)
        
        # Analyze the data
        analyze_combined_data(conn)
        
        # Calculate matching rates for final summary
        total_rows = table_stats['total_rows'].iloc[0]
        rows_with_embryoscope = table_stats['rows_with_embryoscope'].iloc[0]
        rows_with_planilha = table_stats['rows_with_planilha'].iloc[0]
        matched_rows = table_stats['matched_rows'].iloc[0]
        
        match_rate_embryoscope = (matched_rows / rows_with_embryoscope * 100) if rows_with_embryoscope > 0 else 0
        match_rate_planilha = (matched_rows / rows_with_planilha * 100) if rows_with_planilha > 0 else 0
        
        logger.info(f"\nSuccessfully created gold.planilha_embryoscope_combined table")
        logger.info(f"Table contains {total_rows:,} rows")
        logger.info(f"Matched rows: {matched_rows:,}")
        logger.info(f"Match rate (embryoscope): {match_rate_embryoscope:.2f}% | Match rate (planilha): {match_rate_planilha:.2f}%")
        
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

