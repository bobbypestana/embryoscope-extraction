#!/usr/bin/env python3
"""
Join embryo_image_availability_latest to gold.data_ploidia.

This script:
1. Adds api_response_code and api_error_message columns to gold.data_ploidia
2. Updates these columns by joining with silver.embryo_image_availability_latest
"""

import duckdb
import os
import logging
from datetime import datetime
import sys

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
    repo_root = os.path.dirname(os.path.dirname(__file__))
    path_to_db = os.path.join(repo_root, 'database', 'huntington_data_lake.duckdb')
    conn = duckdb.connect(path_to_db, read_only=read_only)
    logger.info(f"Connected to database: {path_to_db} (read_only={read_only})")
    
    # Check if silver.embryo_image_availability_latest exists in this DB or needs attach
    # User context suggests check_columns found silver.embryo_image_availability_latest IN huntington_data_lake
    # But previous scripts attached clinisys_all.
    # Let's verify schema presence.
    try:
        conn.execute("DESCRIBE silver.embryo_image_availability_latest")
        logger.info("Found silver.embryo_image_availability_latest in current database")
    except duckdb.CatalogException:
        logger.warning("silver.embryo_image_availability_latest not found directly.")
        # Try to attach if needed, but assuming it's in huntington_data_lake as per user context
        # (Step 1797 executed purely on huntington_data_lake and found it)
        pass
        
    return conn

def add_columns_if_missing(conn):
    """Add api_response_code and api_error_message columns if they don't exist"""
    logger.info("Checking for new columns...")
    
    # Describe table
    cols = conn.execute("DESCRIBE gold.data_ploidia").fetchall()
    col_names = [c[0] for c in cols]
    
    if "api_response_code" not in col_names:
        logger.info("Adding column api_response_code (INTEGER)...")
        conn.execute("ALTER TABLE gold.data_ploidia ADD COLUMN api_response_code INTEGER")
    else:
        logger.info("Column api_response_code already exists.")
        
    if "api_error_message" not in col_names:
        logger.info("Adding column api_error_message (VARCHAR)...")
        conn.execute("ALTER TABLE gold.data_ploidia ADD COLUMN api_error_message VARCHAR")
    else:
        logger.info("Column api_error_message already exists.")

def update_image_availability(conn):
    """Update api columns from silver.embryo_image_availability_latest"""
    logger.info("=" * 80)
    logger.info("UPDATING IMAGE AVAILABILITY DATA")
    logger.info("=" * 80)
    
    query = """
    UPDATE gold.data_ploidia
    SET 
        api_response_code = s.api_response_code,
        api_error_message = s.error_message
    FROM silver.embryo_image_availability_latest s
    WHERE gold.data_ploidia."Slide ID" = s."embryo_EmbryoID"
    """
    
    # Get count before
    count_query = "SELECT COUNT(*) FROM gold.data_ploidia WHERE api_response_code IS NOT NULL"
    before = conn.execute(count_query).fetchone()[0]
    
    logger.info("Executing UPDATE...")
    result = conn.execute(query).fetchall()
    update_count = result[0][0] if result else "Unknown" # DuckDB UPDATE returns count?
    
    # Get count after
    after = conn.execute(count_query).fetchone()[0]
    
    logger.info(f"Updated {after - before} rows (Total rows with API data: {after})")
    return after

def log_join_metrics(conn):
    """Log metrics about the join quality and response code distribution"""
    logger.info("=" * 80)
    logger.info("JOIN METRICS")
    logger.info("=" * 80)
    
    # Total rows in table
    total_rows = conn.execute("SELECT COUNT(*) FROM gold.data_ploidia").fetchone()[0]
    
    # Rows with data
    matched_rows = conn.execute("SELECT COUNT(*) FROM gold.data_ploidia WHERE api_response_code IS NOT NULL").fetchone()[0]
    
    match_rate = (matched_rows / total_rows * 100) if total_rows > 0 else 0
    
    logger.info(f"Total Rows:   {total_rows:,}")
    logger.info(f"Matched Rows: {matched_rows:,} ({match_rate:.2f}%)")
    logger.info(f"Unmatched:    {total_rows - matched_rows:,}")
    logger.info("")
    
    # Distribution of response codes
    logger.info("Response Code Distribution:")
    logger.info("-" * 40)
    
    query = """
    SELECT 
        COALESCE(CAST(api_response_code AS VARCHAR), 'NULL') as code,
        COUNT(*) as count,
        CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM gold.data_ploidia) AS DECIMAL(5,2)) as pct
    FROM gold.data_ploidia
    GROUP BY api_response_code
    ORDER BY count DESC
    """
    
    distribution = conn.execute(query).fetchall()
    
    if not distribution:
        logger.info("  No data found.")
    else:
        for code, count, pct in distribution:
            # Add description for common codes
            desc = ""
            if code == '200': desc = "(OK)"
            elif code == '404': desc = "(Not Found)"
            elif code == '401': desc = "(Unauthorized)"
            elif code == '500': desc = "(Server Error)"
            
            logger.info(f"  Code {code:<6} {desc:<15}: {count:>6,} ({pct:>5.2f}%)")
            
    logger.info("-" * 40)

def main():
    logger.info("=== JOINING IMAGE AVAILABILITY ===")
    
    try:
        conn = get_database_connection(read_only=False)
        
        add_columns_if_missing(conn)
        
        update_image_availability(conn)
        
        log_join_metrics(conn)
        
        logger.info("")
        logger.info("SUCCESS")
        
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
