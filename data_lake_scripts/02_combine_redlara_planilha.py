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
    repo_root = os.path.dirname(os.path.dirname(__file__))
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
    
    # Create the result table using a waterfall join strategy with tolerance
    join_query = """
    CREATE TABLE gold.redlara_planilha_combined AS
    WITH redlara_with_id AS (
        SELECT *, row_number() OVER() as r_unique_id FROM silver.redlara_unified
    ),
    planilha_with_id AS (
        SELECT *, row_number() OVER() as e_unique_id FROM silver.planilha_embriologia_combined
    ),
    step1 AS (
        -- Step 1: Exact Transfer Date match (valid prontuarios only)
        SELECT r.r_unique_id, e.e_unique_id, 1 as step_id
        FROM redlara_with_id r
        JOIN planilha_with_id e ON r.prontuario = e.prontuario
        WHERE r.prontuario > 0 AND e.prontuario > 0
          AND r.date_of_embryo_transfer = e.fet_data_da_fet AND r.date_of_embryo_transfer IS NOT NULL
    ),
    step2 AS (
        -- Step 2: Transfer Date match (with 3-day tolerance)
        SELECT r.r_unique_id, e.e_unique_id, 2 as step_id
        FROM redlara_with_id r
        JOIN planilha_with_id e ON r.prontuario = e.prontuario
        WHERE r.prontuario > 0 AND e.prontuario > 0
          AND date_diff('day', r.date_of_embryo_transfer, e.fet_data_da_fet) BETWEEN -3 AND 3
          AND r.date_of_embryo_transfer IS NOT NULL AND e.fet_data_da_fet IS NOT NULL
          AND r.r_unique_id NOT IN (SELECT r_unique_id FROM step1)
          AND e.e_unique_id NOT IN (SELECT e_unique_id FROM step1)
    ),
    step3 AS (
        -- Step 3: Cryo Date FET match (Exact)
        SELECT r.r_unique_id, e.e_unique_id, 3 as step_id
        FROM redlara_with_id r
        JOIN planilha_with_id e ON r.prontuario = e.prontuario
        WHERE r.prontuario > 0 AND e.prontuario > 0
          AND r.date_when_embryos_were_cryopreserved = e.fet_data_crio AND r.date_when_embryos_were_cryopreserved IS NOT NULL
          AND r.r_unique_id NOT IN (SELECT r_unique_id FROM step1 UNION SELECT r_unique_id FROM step2)
          AND e.e_unique_id NOT IN (SELECT e_unique_id FROM step1 UNION SELECT e_unique_id FROM step2)
    ),
    step4 AS (
        -- Step 4: Cryo Date FET match (with 3-day tolerance)
        SELECT r.r_unique_id, e.e_unique_id, 4 as step_id
        FROM redlara_with_id r
        JOIN planilha_with_id e ON r.prontuario = e.prontuario
        WHERE r.prontuario > 0 AND e.prontuario > 0
          AND date_diff('day', r.date_when_embryos_were_cryopreserved, e.fet_data_crio) BETWEEN -3 AND 3
          AND r.date_when_embryos_were_cryopreserved IS NOT NULL AND e.fet_data_crio IS NOT NULL
          AND r.r_unique_id NOT IN (SELECT r_unique_id FROM step1 UNION SELECT r_unique_id FROM step2 UNION SELECT r_unique_id FROM step3)
          AND e.e_unique_id NOT IN (SELECT e_unique_id FROM step1 UNION SELECT e_unique_id FROM step2 UNION SELECT e_unique_id FROM step3)
    ),
    step5 AS (
        -- Step 5: Cryo Date Fresh match (Exact)
        SELECT r.r_unique_id, e.e_unique_id, 5 as step_id
        FROM redlara_with_id r
        JOIN planilha_with_id e ON r.prontuario = e.prontuario
        WHERE r.prontuario > 0 AND e.prontuario > 0
          AND r.date_when_embryos_were_cryopreserved = e.fresh_data_crio AND r.date_when_embryos_were_cryopreserved IS NOT NULL
          AND r.r_unique_id NOT IN (SELECT r_unique_id FROM step1 UNION SELECT r_unique_id FROM step2 UNION SELECT r_unique_id FROM step3 UNION SELECT r_unique_id FROM step4)
          AND e.e_unique_id NOT IN (SELECT e_unique_id FROM step1 UNION SELECT e_unique_id FROM step2 UNION SELECT e_unique_id FROM step3 UNION SELECT e_unique_id FROM step4)
    ),
    step6 AS (
        -- Step 6: Cryo Date Fresh match (with 3-day tolerance)
        SELECT r.r_unique_id, e.e_unique_id, 6 as step_id
        FROM redlara_with_id r
        JOIN planilha_with_id e ON r.prontuario = e.prontuario
        WHERE r.prontuario > 0 AND e.prontuario > 0
          AND date_diff('day', r.date_when_embryos_were_cryopreserved, e.fresh_data_crio) BETWEEN -3 AND 3
          AND r.date_when_embryos_were_cryopreserved IS NOT NULL AND e.fresh_data_crio IS NOT NULL
          AND r.r_unique_id NOT IN (SELECT r_unique_id FROM step1 UNION SELECT r_unique_id FROM step2 UNION SELECT r_unique_id FROM step3 UNION SELECT r_unique_id FROM step4 UNION SELECT r_unique_id FROM step5)
          AND e.e_unique_id NOT IN (SELECT e_unique_id FROM step1 UNION SELECT e_unique_id FROM step2 UNION SELECT e_unique_id FROM step3 UNION SELECT e_unique_id FROM step4 UNION SELECT e_unique_id FROM step5)
    ),
    all_matches AS (
        SELECT * FROM step1
        UNION ALL SELECT * FROM step2
        UNION ALL SELECT * FROM step3
        UNION ALL SELECT * FROM step4
        UNION ALL SELECT * FROM step5
        UNION ALL SELECT * FROM step6
    ),
    best_matches AS (
        SELECT * FROM (
            SELECT 
                *,
                ROW_NUMBER() OVER(PARTITION BY r_unique_id ORDER BY step_id) as r_rank,
                ROW_NUMBER() OVER(PARTITION BY e_unique_id ORDER BY step_id) as e_rank
            FROM all_matches
        ) WHERE r_rank = 1 AND e_rank = 1
    ),
    combined AS (
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
            m.step_id as redlara_planilha_join_step,
            r.* EXCLUDE (prontuario, date_of_embryo_transfer, number_of_newborns, r_unique_id),
            e.* EXCLUDE (prontuario, fet_data_da_fet, fet_no_nascidos, e_unique_id)
        FROM redlara_with_id r
        FULL OUTER JOIN best_matches m ON r.r_unique_id = m.r_unique_id
        FULL OUTER JOIN planilha_with_id e ON m.e_unique_id = e.e_unique_id
    )
    SELECT * FROM combined WHERE prontuario IS NOT NULL
    """
    
    logger.info("Executing Waterfall Join to create gold table...")
    conn.execute(join_query)
    
    # Calculate statistics directly from output and inputs
    total_planilha = conn.execute("SELECT COUNT(*) FROM silver.planilha_embriologia_combined").fetchone()[0]
    total_redlara = conn.execute("SELECT COUNT(*) FROM silver.redlara_unified").fetchone()[0]
    
    matched_count = conn.execute("""
        SELECT COUNT(*) 
        FROM gold.redlara_planilha_combined 
        WHERE redlara_planilha_join_step IS NOT NULL
    """).fetchone()[0]
    
    total_gold = conn.execute("SELECT COUNT(*) FROM gold.redlara_planilha_combined").fetchone()[0]
    
    stats_by_step = conn.execute("""
        SELECT redlara_planilha_join_step, COUNT(*) as count
        FROM gold.redlara_planilha_combined
        WHERE redlara_planilha_join_step IS NOT NULL
        GROUP BY redlara_planilha_join_step
        ORDER BY redlara_planilha_join_step
    """).df()
    
    # Log Final Stats
    logger.info("=" * 60)
    logger.info("JOIN STATISTICS SUMMARY")
    logger.info("=" * 60)
    logger.info(f"PLANILHA PERSPECTIVE (Total Rows: {total_planilha:,}):")
    logger.info(f"  Matched with Redlara: {matched_count:,}")
    logger.info(f"  Match Rate: {(matched_count / total_planilha * 100):.2f}%")
    
    logger.info("-" * 60)
    logger.info(f"REDLARA PERSPECTIVE (Total Rows: {total_redlara:,}):")
    logger.info(f"  Matched with Planilha: {matched_count:,}")
    logger.info(f"  Match Rate: {(matched_count / total_redlara * 100):.2f}%")
    
    logger.info("-" * 60)
    logger.info("Matches by Waterfall Step:")
    step_labels = {
        1: "Exact Transfer Date",
        2: "Transfer Date (+/- 3 days)",
        3: "Exact Cryo Date (FET)",
        4: "Cryo Date (FET) (+/- 3 days)",
        5: "Exact Cryo Date (Fresh)",
        6: "Cryo Date (Fresh) (+/- 3 days)"
    }
    for _, row in stats_by_step.iterrows():
        step = int(row['redlara_planilha_join_step'])
        label = step_labels.get(step, f"Step {step}")
        logger.info(f"  Step {step}: {label:<35} | {int(row['count']):>6,}")
        
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
