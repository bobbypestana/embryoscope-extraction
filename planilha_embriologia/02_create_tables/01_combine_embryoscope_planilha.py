#!/usr/bin/env python3
"""
Combine gold.embryoscope_clinisys_combined with silver.planilha_embriologia_fresh
Join conditions:
- micro_prontuario = prontuario (normalized)
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
    """Create the gold.planilha_embryoscope_combined table with both LEFT and RIGHT join metrics"""
    
    logger.info("Creating gold.planilha_embryoscope_combined table...")
    
    # Drop table if it exists
    conn.execute("DROP TABLE IF EXISTS gold.planilha_embryoscope_combined")
    
    # 1. First, create the combined table using a FULL OUTER JOIN to calculate metrics for both sides
    # We include all columns from embryoscope and specific columns from planilha
    create_table_query = """
    CREATE TABLE gold.planilha_embryoscope_combined AS
    SELECT 
        e.*,
        p."FATOR 1" as planilha_fator_1,
        p."INCUBADORA" as planilha_incubadora,
        p.prontuario as planilha_prontuario,
        p."DATA DA PUNÇÃO" as planilha_data_puncao
    FROM gold.embryoscope_clinisys_combined e
    FULL OUTER JOIN silver.planilha_embriologia_fresh p
        ON e.micro_prontuario = p.prontuario
        AND CAST(e.micro_Data_DL AS DATE) = CAST(p."DATA DA PUNÇÃO" AS DATE)
    """
    
    logger.info("Executing Full Outer Join to build the combined table and metrics...")
    conn.execute(create_table_query)
    
    # 2. Calculate Metrics
    stats = conn.execute("""
        SELECT 
            COUNT(*) as total_rows,
            -- Coverage for Left Join (Embryoscope rows found in Planilha)
            COUNT(CASE WHEN micro_prontuario IS NOT NULL THEN 1 END) as total_embryoscope,
            COUNT(CASE WHEN micro_prontuario IS NOT NULL AND planilha_prontuario IS NOT NULL THEN 1 END) as embryoscope_matched,
            
            -- Coverage for Right Join (Planilha rows found in Embryoscope)
            COUNT(CASE WHEN planilha_prontuario IS NOT NULL THEN 1 END) as total_planilha,
            COUNT(CASE WHEN planilha_prontuario IS NOT NULL AND micro_prontuario IS NOT NULL THEN 1 END) as planilha_matched
        FROM gold.planilha_embryoscope_combined
    """).df()
    
    total_embryoscope = stats['total_embryoscope'].iloc[0]
    embryoscope_matched = stats['embryoscope_matched'].iloc[0]
    total_planilha = stats['total_planilha'].iloc[0]
    planilha_matched = stats['planilha_matched'].iloc[0]
    
    embryoscope_coverage = (embryoscope_matched / total_embryoscope * 100) if total_embryoscope > 0 else 0
    planilha_coverage = (planilha_matched / total_planilha * 100) if total_planilha > 0 else 0
    
    logger.info("=" * 60)
    logger.info("COMBINATION METRICS")
    logger.info("=" * 60)
    logger.info(f"LEFT JOIN (Embryoscope -> Planilha):")
    logger.info(f"   Total Embryoscope Rows: {total_embryoscope:,}")
    logger.info(f"   Matched in Planilha:    {embryoscope_matched:,}")
    logger.info(f"   Coverage Rate:          {embryoscope_coverage:.2f}%")
    logger.info("-" * 60)
    logger.info(f"RIGHT JOIN (Planilha -> Embryoscope):")
    logger.info(f"   Total Planilha Rows:    {total_planilha:,}")
    logger.info(f"   Matched in Embryoscope: {planilha_matched:,}")
    logger.info(f"   Coverage Rate:          {planilha_coverage:.2f}%")
    logger.info("=" * 60)
    
    # 3. According to request, we want the LEFT join result as the final table (Embryoscope is the left table)
    # However, since we already did a FULL OUTER JOIN, the user might want a filtered version or keep all
    # Usually, a combined table in gold is the result of the desired join direction.
    # The user said "embryoscope_clinisys_combined is the left table in the join".
    # I will recreate it as a LEFT JOIN to keep only Embryoscope records (enriched with Planilha data where found)
    
    logger.info("Recreating table as LEFT JOIN (keeping all Embryoscope records)...")
    conn.execute("DROP TABLE gold.planilha_embryoscope_combined")
    
    final_join_query = """
    CREATE TABLE gold.planilha_embryoscope_combined AS
    SELECT 
        e.*,
        p."FATOR 1" as planilha_fator_1,
        p."INCUBADORA" as planilha_incubadora
    FROM gold.embryoscope_clinisys_combined e
    LEFT JOIN silver.planilha_embriologia_fresh p
        ON e.micro_prontuario = p.prontuario
        AND CAST(e.micro_Data_DL AS DATE) = CAST(p."DATA DA PUNÇÃO" AS DATE)
    """
    conn.execute(final_join_query)
    logger.info(f"Final table gold.planilha_embryoscope_combined created with {total_embryoscope:,} rows")

def main():
    logger.info("=== JOINING EMBRYOSCOPE WITH PLANILHA FRESH ===")
    try:
        conn = get_database_connection()
        create_combined_table(conn)
        logger.info("Success!")
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
