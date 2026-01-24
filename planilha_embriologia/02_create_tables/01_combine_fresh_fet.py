#!/usr/bin/env python3
"""
01_combine_fresh_fet.py
Merges silver.planilha_embriologia_fresh (left) and silver.planilha_embriologia_fet (right)
Join conditions:
- prontuario matches
- Cryopreservation date matches within +/- 1 day tolerance
"""

import duckdb as db
import os
import logging
from datetime import datetime

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

def get_database_connection():
    """Create and return a connection to the huntington_data_lake database"""
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    path_to_db = os.path.join(repo_root, 'database', 'huntington_data_lake.duckdb')
    conn = db.connect(path_to_db)
    logger.info(f"Connected to database: {path_to_db}")
    return conn

def combine_fresh_fet(conn):
    """Combine FRESH and FET tables with 1-day tolerance join on cryo date"""
    
    logger.info("Merging FRESH and FET cycles...")
    
    # Drop existing table
    conn.execute("DROP TABLE IF EXISTS silver.planilha_embriologia_combined")
    
    # Selected columns from FRESH: prontuario, data_da_puncao, fator_1, incubadora, data_cryo
    # Selected columns from FET: prontuario, data_da_fet, data_crio, result (fet_resultado), tipo_do_resultado (fet_tipo_resultado), no_nascidos (fet_no_nascidos)
    
    query = """
    CREATE TABLE silver.planilha_embriologia_combined AS
    SELECT 
        f.prontuario,
        f.data_da_puncao,
        f.fator_1,
        f.incubadora,
        f.data_crio as fresh_data_crio,
        t.data_da_fet,
        t.data_crio as fet_data_crio,
        t.result as fet_resultado,
        t.tipo_do_resultado as fet_tipo_resultado,
        t.no_nascidos as fet_no_nascidos
    FROM silver.planilha_embriologia_fresh f
    LEFT JOIN silver.planilha_embriologia_fet t
        ON f.prontuario = t.prontuario
        AND ABS(date_diff('day', TRY_CAST(f.data_crio AS TIMESTAMP), TRY_CAST(t.data_crio AS TIMESTAMP))) <= 1
    """
    
    conn.execute(query)
    
    # Coverage Stats
    stats = conn.execute("""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(CASE WHEN fet_data_crio IS NOT NULL THEN 1 END) as matches
        FROM silver.planilha_embriologia_combined
    """).df().iloc[0]
    
    total = stats['total_rows']
    matches = stats['matches']
    rate = (matches / total * 100) if total > 0 else 0
    
    logger.info(f"Merge completed: {total:,} rows created.")
    logger.info(f"Matches found: {matches:,} ({rate:.2f}%)")
    logger.info("Result saved to silver.planilha_embriologia_combined")

def main():
    logger.info("=== STARTING FRESH-FET MERGER (1-DAY TOLERANCE) ===")
    try:
        conn = get_database_connection()
        combine_fresh_fet(conn)
        logger.info("Success!")
    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
