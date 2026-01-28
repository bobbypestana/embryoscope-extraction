#!/usr/bin/env python3
"""
02_combine_redlara_planilha.py
Combines silver.redlara_unified with silver.planilha_embriologia_combined (result of 01_combine_fresh_fet.py)

Join conditions:
- redlara_unified.prontuario = planilha_embriologia_combined.prontuario (INTEGER)
- redlara_unified.date_of_embryo_transfer = planilha_embriologia_combined.data_da_fet (DATE)
"""

import duckdb as db
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
    """Create the gold.redlara_planilha_combined table and log join statistics"""
    
    logger.info("Creating gold.redlara_planilha_combined table...")
    
    # Ensure gold schema exists
    conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
    
    # Drop table if it exists
    conn.execute("DROP TABLE IF EXISTS gold.redlara_planilha_combined")
    
    # FULL JOIN is used to preserve all records from both sides for reporting
    # But often users want a unified view. Here we'll create a join
    # and provide the requested statistics.
    
    # Calculating Statistics FIRST
    logger.info("Calculating Join Statistics...")
    
    # 1. Left Join: Planilha -> Redlara
    stats_left = conn.execute("""
        SELECT 
            COUNT(*) as total_embriologia,
            SUM(CASE WHEN r.prontuario IS NOT NULL THEN 1 ELSE 0 END) as matched
        FROM silver.planilha_embriologia_combined e
        LEFT JOIN silver.redlara_unified r
          ON e.prontuario = r.prontuario
          AND (
               (r.date_of_embryo_transfer = e.fet_data_da_fet AND r.date_of_embryo_transfer IS NOT NULL)
               OR (r.date_when_embryos_were_cryopreserved = e.fet_data_crio AND r.date_when_embryos_were_cryopreserved IS NOT NULL)
               OR (r.date_when_embryos_were_cryopreserved = e.fresh_data_crio AND r.date_when_embryos_were_cryopreserved IS NOT NULL)
          )
    """).df().iloc[0]
    
    # 2. Right Join: Redlara -> Planilha
    stats_right = conn.execute("""
        SELECT 
            COUNT(*) as total_redlara,
            SUM(CASE WHEN e.prontuario IS NOT NULL THEN 1 ELSE 0 END) as matched
        FROM silver.redlara_unified r
        LEFT JOIN silver.planilha_embriologia_combined e
          ON r.prontuario = e.prontuario
          AND (
               (r.date_of_embryo_transfer = e.fet_data_da_fet AND r.date_of_embryo_transfer IS NOT NULL)
               OR (r.date_when_embryos_were_cryopreserved = e.fet_data_crio AND r.date_when_embryos_were_cryopreserved IS NOT NULL)
               OR (r.date_when_embryos_were_cryopreserved = e.fresh_data_crio AND r.date_when_embryos_were_cryopreserved IS NOT NULL)
          )
    """).df().iloc[0]
    
    # Create the result table using a priority-based multi-step join
    join_query = """
    CREATE TABLE gold.redlara_planilha_combined AS
    WITH redlara_with_id AS (
        SELECT *, row_number() OVER() as r_unique_id FROM silver.redlara_unified
    ),
    planilha_with_id AS (
        SELECT *, row_number() OVER() as e_unique_id FROM silver.planilha_embriologia_combined
    ),
    potential_matches AS (
        SELECT 
            r.r_unique_id,
            e.e_unique_id,
            CASE 
                WHEN r.date_of_embryo_transfer = e.fet_data_da_fet AND r.date_of_embryo_transfer IS NOT NULL THEN 1
                WHEN r.date_when_embryos_were_cryopreserved = e.fet_data_crio AND r.date_when_embryos_were_cryopreserved IS NOT NULL THEN 2
                WHEN r.date_when_embryos_were_cryopreserved = e.fresh_data_crio AND r.date_when_embryos_were_cryopreserved IS NOT NULL THEN 3
                ELSE 99
            END as match_priority
        FROM redlara_with_id r
        JOIN planilha_with_id e ON r.prontuario = e.prontuario
        WHERE match_priority < 99
    ),
    best_matches AS (
        SELECT * FROM (
            SELECT 
                *,
                -- Select best match for each Redlara record
                ROW_NUMBER() OVER(PARTITION BY r_unique_id ORDER BY match_priority) as r_rank,
                -- Select best match for each Planilha record
                ROW_NUMBER() OVER(PARTITION BY e_unique_id ORDER BY match_priority) as e_rank
            FROM potential_matches
        ) WHERE r_rank = 1 AND e_rank = 1
    )
    SELECT 
        COALESCE(r.prontuario, e.prontuario) as prontuario,
        COALESCE(r.date_of_embryo_transfer, e.fet_data_da_fet) as transfer_date,
        COALESCE(TRY_CAST(r.number_of_newborns AS INTEGER), TRY_CAST(e.fet_no_nascidos AS INTEGER)) as merged_numero_de_nascidos,
        CASE 
            WHEN e.fresh_incubadora ILIKE '%ES%' THEN 'Embryoscope'
            WHEN e.fresh_incubadora ILIKE '%THERMO%' THEN 'THERMO'
            WHEN e.fresh_incubadora IS NOT NULL THEN 'K-SYSTEM'
            ELSE NULL 
        END as incubadora_padronizada,
        r.* EXCLUDE (prontuario, date_of_embryo_transfer, number_of_newborns, r_unique_id),
        e.* EXCLUDE (prontuario, fet_data_da_fet, fet_no_nascidos, e_unique_id)
    FROM redlara_with_id r
    FULL OUTER JOIN best_matches m ON r.r_unique_id = m.r_unique_id
    FULL OUTER JOIN planilha_with_id e ON m.e_unique_id = e.e_unique_id
    """
    
    logger.info("Executing Full Outer Join to create gold table...")
    conn.execute(join_query)
    
    total_gold = conn.execute("SELECT COUNT(*) FROM gold.redlara_planilha_combined").fetchone()[0]
    
    # Log Final Stats
    logger.info("=" * 60)
    logger.info("JOIN STATISTICS SUMMARY")
    logger.info("=" * 60)
    logger.info("LEFT JOIN (Planilha -> Redlara):")
    logger.info(f"  Total Planilha Rows: {int(stats_left['total_embriologia']):,}")
    logger.info(f"  Matched with Redlara: {int(stats_left['matched']):,}")
    logger.info(f"  Match Rate: {(stats_left['matched'] / stats_left['total_embriologia'] * 100):.2f}%")
    
    logger.info("-" * 60)
    logger.info("RIGHT JOIN (Redlara -> Planilha):")
    logger.info(f"  Total Redlara Rows: {int(stats_right['total_redlara']):,}")
    logger.info(f"  Matched with Planilha: {int(stats_right['matched']):,}")
    logger.info(f"  Match Rate: {(stats_right['matched'] / stats_right['total_redlara'] * 100):.2f}%")
    
    logger.info("=" * 60)
    logger.info(f"Gold table gold.redlara_planilha_combined created with {total_gold:,} rows.")
    logger.info("=" * 60)

def main():
    logger.info("=== MERGING REDLARA UNIFIED AND PLANILHA COMBINED ===")
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
