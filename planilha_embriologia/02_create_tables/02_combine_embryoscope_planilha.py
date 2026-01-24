#!/usr/bin/env python3
"""
02_combine_embryoscope_planilha.py
Combines gold.embryoscope_clinisys_combined with the merged Planilha data 
(silver.planilha_embriologia_combined - result of 01_combine_fresh_fet.py)

Join conditions:
- micro_prontuario = prontuario
- micro_Data_DL = "DATA DA PUNÇÃO"
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

def get_database_connection(read_only=False):
    """Create and return a connection to the huntington_data_lake database"""
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    path_to_db = os.path.join(repo_root, 'database', 'huntington_data_lake.duckdb')
    conn = db.connect(path_to_db, read_only=read_only)
    logger.info(f"Connected to database: {path_to_db} (read_only={read_only})")
    return conn

def create_combined_table(conn):
    """Create the gold.planilha_embryoscope_combined table enriched with Planilha data"""
    
    logger.info("Creating gold.planilha_embryoscope_combined table...")
    
    # Drop table if it exists
    conn.execute("DROP TABLE IF EXISTS gold.planilha_embryoscope_combined")
    
    # We join Embryoscope with the already merged Planilha Combined (Fresh + FET)
    # Keeping Embryoscope as the LEFT table
    join_query = """
    CREATE TABLE gold.planilha_embryoscope_combined AS
    SELECT 
        e.*,
        p.fator_1 as planilha_fator_1,
        p.incubadora as planilha_incubadora,
        p.data_da_fet as planilha_data_fet,
        p.fet_resultado as planilha_resultado_final,
        p.fet_tipo_resultado as planilha_tipo_resultado,
        p.fet_no_nascidos as planilha_n_nascidos
    FROM gold.embryoscope_clinisys_combined e
    LEFT JOIN silver.planilha_embriologia_combined p
        ON e.micro_prontuario = p.prontuario
        AND CAST(e.micro_Data_DL AS DATE) = CAST(p.data_da_puncao AS DATE)
    """
    
    logger.info("Executing Left Join between Embryoscope and Combined Planilha...")
    conn.execute(join_query)
    
    # Calculate Metrics
    stats = conn.execute("""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(CASE WHEN planilha_data_fet IS NOT NULL THEN 1 END) as matched_with_planilha
        FROM gold.planilha_embryoscope_combined
    """).df().iloc[0]
    
    total = stats['total_rows']
    matched = stats['matched_with_planilha']
    rate = (matched / total * 100) if total > 0 else 0
    
    logger.info("=" * 60)
    logger.info("COMBINATION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total Rows in Gold Table: {total:,}")
    logger.info(f"Enriched with Planilha Data: {matched:,} ({rate:.2f}%)")
    logger.info("=" * 60)

def main():
    logger.info("=== JOINING EMBRYOSCOPE WITH MERGED PLANILHA DATA ===")
    try:
        conn = get_database_connection()
        create_combined_table(conn)
        logger.info("Success!")
    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
