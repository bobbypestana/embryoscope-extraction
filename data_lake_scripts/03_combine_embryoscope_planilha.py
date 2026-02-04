#!/usr/bin/env python3
"""
03_combine_embryoscope_planilha.py
Combines gold.embryoscope_clinisys_combined with the unified Redlara-Planilha data 
(gold.redlara_planilha_combined) using a waterfall join strategy.

Join Conditions (Waterfall Priority):
1. prontuario + data_da_puncao (Embryoscope micro_Data_DL)
2. prontuario + transfer_date (Embryoscope descong_em_DataTransferencia)
3. prontuario + transfer_date (Embryoscope trat1_data_transferencia)
4. prontuario + transfer_date (Embryoscope trat2_data_transferencia)
5. prontuario + cryo_date (Redlara) = cong_em_Data
6. prontuario + cryo_date (FET) = cong_em_Data
7. prontuario + cryo_date (Fresh) = cong_em_Data
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
    repo_root = os.path.dirname(os.path.dirname(__file__))
    path_to_db = os.path.join(repo_root, 'database', 'huntington_data_lake.duckdb')
    conn = db.connect(path_to_db, read_only=read_only)
    logger.info(f"Connected to database: {path_to_db} (read_only={read_only})")
    return conn

def create_combined_table(conn):
    """Create the gold.planilha_embryoscope_combined table with waterfall logic"""
    
    logger.info("Executing Waterfall Join between Embryoscope and Redlara-Planilha...")
    
    # Drop existing table
    conn.execute("DROP TABLE IF EXISTS gold.planilha_embryoscope_combined")
    
    # We use a series of CTEs to identify matches at each level
    query = """
    CREATE TABLE gold.planilha_embryoscope_combined AS
    WITH emb AS (
        SELECT *, 
               CAST(micro_prontuario AS INTEGER) as prontuario_int,
               CAST(micro_Data_DL AS DATE) as puncao_date,
               CAST(descong_em_DataTransferencia AS DATE) as transfer_date_descong,
               CAST(trat1_data_transferencia AS DATE) as transfer_date_trat1,
               CAST(trat2_data_transferencia AS DATE) as transfer_date_trat2,
               CAST(cong_em_Data AS DATE) as cong_date
        FROM gold.embryoscope_clinisys_combined
    ),
    src AS (
        SELECT *, ROW_NUMBER() OVER() as src_row_id
        FROM gold.redlara_planilha_combined
    ),
    step1 AS (
        -- Step 1: Punção (micro_Data_DL) matches fresh_data_da_puncao (PRIORITY)
        SELECT e.oocito_id, s.src_row_id, 1 as step_id
        FROM emb e JOIN src s ON e.prontuario_int = s.prontuario AND e.puncao_date = s.fresh_data_da_puncao
        WHERE e.puncao_date IS NOT NULL AND s.fresh_data_da_puncao IS NOT NULL
    ),
    step2 AS (
        -- Step 2: descong_em_DataTransferencia = transfer_date
        SELECT e.oocito_id, s.src_row_id, 2 as step_id
        FROM emb e JOIN src s ON e.prontuario_int = s.prontuario AND e.transfer_date_descong = s.transfer_date
        WHERE e.transfer_date_descong IS NOT NULL AND s.transfer_date IS NOT NULL
          AND e.oocito_id NOT IN (SELECT oocito_id FROM step1)
    ),
    step3 AS (
        -- Step 3: trat1_data_transferencia = transfer_date
        SELECT e.oocito_id, s.src_row_id, 3 as step_id
        FROM emb e JOIN src s ON e.prontuario_int = s.prontuario AND e.transfer_date_trat1 = s.transfer_date
        WHERE e.transfer_date_trat1 IS NOT NULL AND s.transfer_date IS NOT NULL
          AND e.oocito_id NOT IN (SELECT oocito_id FROM step1 UNION SELECT oocito_id FROM step2)
    ),
    step4 AS (
        -- Step 4: trat2_data_transferencia = transfer_date
        SELECT e.oocito_id, s.src_row_id, 4 as step_id
        FROM emb e JOIN src s ON e.prontuario_int = s.prontuario AND e.transfer_date_trat2 = s.transfer_date
        WHERE e.transfer_date_trat2 IS NOT NULL AND s.transfer_date IS NOT NULL
          AND e.oocito_id NOT IN (SELECT oocito_id FROM step1 UNION SELECT oocito_id FROM step2 UNION SELECT oocito_id FROM step3)
    ),
    step5 AS (
        -- Step 5: cong_em_Data = date_when_embryos_were_cryopreserved
        SELECT e.oocito_id, s.src_row_id, 5 as step_id
        FROM emb e JOIN src s ON e.prontuario_int = s.prontuario AND e.cong_date = s.date_when_embryos_were_cryopreserved
        WHERE e.cong_date IS NOT NULL AND s.date_when_embryos_were_cryopreserved IS NOT NULL
          AND e.oocito_id NOT IN (SELECT oocito_id FROM step1 UNION SELECT oocito_id FROM step2 UNION SELECT oocito_id FROM step3 UNION SELECT oocito_id FROM step4)
    ),
    step6 AS (
        -- Step 6: cong_em_Data = fet_data_crio
        SELECT e.oocito_id, s.src_row_id, 6 as step_id
        FROM emb e JOIN src s ON e.prontuario_int = s.prontuario AND e.cong_date = s.fet_data_crio
        WHERE e.cong_date IS NOT NULL AND s.fet_data_crio IS NOT NULL
          AND e.oocito_id NOT IN (SELECT oocito_id FROM step1 UNION SELECT oocito_id FROM step2 UNION SELECT oocito_id FROM step3 UNION SELECT oocito_id FROM step4 UNION SELECT oocito_id FROM step5)
    ),
    step7 AS (
        -- Step 7: cong_em_Data = fresh_data_crio
        SELECT e.oocito_id, s.src_row_id, 7 as step_id
        FROM emb e JOIN src s ON e.prontuario_int = s.prontuario AND e.cong_date = s.fresh_data_crio
        WHERE e.cong_date IS NOT NULL AND s.fresh_data_crio IS NOT NULL
          AND e.oocito_id NOT IN (SELECT oocito_id FROM step1 UNION SELECT oocito_id FROM step2 UNION SELECT oocito_id FROM step3 UNION SELECT oocito_id FROM step4 UNION SELECT oocito_id FROM step5 UNION SELECT oocito_id FROM step6)
    ),
    all_matches AS (
        SELECT * FROM step1 
        UNION ALL SELECT * FROM step2 
        UNION ALL SELECT * FROM step3 
        UNION ALL SELECT * FROM step4 
        UNION ALL SELECT * FROM step5 
        UNION ALL SELECT * FROM step6 
        UNION ALL SELECT * FROM step7
    ),
    step8 AS (
        -- Step 8: Punção +/- 3 days
        SELECT e.oocito_id, s.src_row_id, 8 as step_id
        FROM emb e JOIN src s ON e.prontuario_int = s.prontuario
        WHERE date_diff('day', CAST(s.fresh_data_da_puncao AS TIMESTAMP), CAST(e.puncao_date AS TIMESTAMP)) BETWEEN -3 AND 3
          AND e.oocito_id NOT IN (SELECT oocito_id FROM all_matches)
    ),
    step9 AS (
        -- Step 9: Transfer +/- 3 days
        SELECT e.oocito_id, s.src_row_id, 9 as step_id
        FROM emb e JOIN src s ON e.prontuario_int = s.prontuario
        WHERE (
            date_diff('day', CAST(s.transfer_date AS TIMESTAMP), CAST(e.transfer_date_descong AS TIMESTAMP)) BETWEEN -3 AND 3 OR
            date_diff('day', CAST(s.transfer_date AS TIMESTAMP), CAST(e.transfer_date_trat1 AS TIMESTAMP)) BETWEEN -3 AND 3 OR
            date_diff('day', CAST(s.transfer_date AS TIMESTAMP), CAST(e.transfer_date_trat2 AS TIMESTAMP)) BETWEEN -3 AND 3
        )
          AND e.oocito_id NOT IN (SELECT oocito_id FROM all_matches UNION SELECT oocito_id FROM step8)
    ),
    all_combined_matches AS (
        SELECT * FROM all_matches
        UNION ALL SELECT * FROM step8
        UNION ALL SELECT * FROM step9
    ),
    final_matches AS (
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY oocito_id ORDER BY step_id) as rn
            FROM all_combined_matches
        ) WHERE rn = 1
    )
    SELECT 
        e.*,
        m.step_id as join_step,
        m.src_row_id as matched_src_row_id,
        s.prontuario as matched_planilha_prontuario,
        s.transfer_date as matched_planilha_transfer_date,
        s.* EXCLUDE (src_row_id, prontuario, transfer_date, fresh_data_da_puncao, date_when_embryos_were_cryopreserved)
    FROM gold.embryoscope_clinisys_combined e
    LEFT JOIN final_matches m ON e.oocito_id = m.oocito_id
    LEFT JOIN src s ON m.src_row_id = s.src_row_id
    """
    
    conn.execute(query)
    
    # Calculate Metrics
    stats_overall = conn.execute("""
        SELECT 
            COUNT(*) as total_embryos,
            COUNT(join_step) as matched_embryos
        FROM gold.planilha_embryoscope_combined
    """).df().iloc[0]
    
    stats_by_step = conn.execute("""
        SELECT join_step, COUNT(*) as count
        FROM gold.planilha_embryoscope_combined
        WHERE join_step IS NOT NULL
        GROUP BY join_step
        ORDER BY join_step
    """).df()

    # Planilha-side metrics
    stats_planilha = conn.execute("""
        SELECT 
            (SELECT COUNT(*) FROM gold.redlara_planilha_combined) as total_src,
            COUNT(DISTINCT matched_src_row_id) as matched_src
        FROM gold.planilha_embryoscope_combined
        WHERE join_step IS NOT NULL
    """).df().iloc[0]
    
    total = stats_overall['total_embryos']
    matched = stats_overall['matched_embryos']
    rate = (matched / total * 100) if total > 0 else 0

    total_src = stats_planilha['total_src']
    matched_src = stats_planilha['matched_src']
    rate_src = (matched_src / total_src * 100) if total_src > 0 else 0
    
    logger.info("=" * 60)
    logger.info("WATERFALL JOIN SUMMARY (Embryoscope -> Planilha-Redlara)")
    logger.info("=" * 60)
    logger.info(f"EMBRYOSCOPE PERSPECTIVE:")
    logger.info(f"  Total Embryos: {total:,}")
    logger.info(f"  Matched:       {matched:,} ({rate:.2f}%)")
    logger.info("-" * 60)
    logger.info("PLANILHA-REDLARA PERSPECTIVE:")
    logger.info(f"  Total Records: {total_src:,}")
    logger.info(f"  Matched:       {matched_src:,} ({rate_src:.2f}%)")
    logger.info("-" * 60)
    logger.info("Matches by Waterfall Step (Embryos):")
    
    step_labels = {
        1: "Punção (Date DL)",
        2: "Transfer (Descong)",
        3: "Transfer (Trat1)",
        4: "Transfer (Trat2)",
        5: "Cryo (Redlara)",
        6: "Cryo (FET)",
        7: "Cryo (Fresh)",
        8: "Punção (+/- 3 days)",
        9: "Transfer (+/- 3 days)"
    }
    
    for _, row in stats_by_step.iterrows():
        step = int(row['join_step'])
        label = step_labels.get(step, f"Step {step}")
        logger.info(f"  Step {step}: {label:<25} | {int(row['count']):>6,}")
    
    logger.info("=" * 60)
    logger.info("Gold table gold.planilha_embryoscope_combined created.")
    
    # Yearly Match Analysis
    stats_by_year = conn.execute("""
    WITH src_numbered AS (
        SELECT 
            *, 
            ROW_NUMBER() OVER() as src_id,
            COALESCE(fresh_file_name, fet_file_name, 'Unknown') as file_source,
            EXTRACT(YEAR FROM transfer_date) as date_year
        FROM gold.redlara_planilha_combined
    ),
    matched_ids AS (
        SELECT DISTINCT matched_src_row_id FROM gold.planilha_embryoscope_combined WHERE matched_src_row_id IS NOT NULL
    ),
    src_with_year AS (
        SELECT 
            *,
            CASE 
                WHEN file_source LIKE '%2021%' THEN 2021
                WHEN file_source LIKE '%2022%' THEN 2022
                WHEN file_source LIKE '%2023%' THEN 2023
                WHEN file_source LIKE '%2024%' THEN 2024
                WHEN file_source LIKE '%2025%' THEN 2025
                ELSE date_year
            END as year_group
        FROM src_numbered
    ),
    src_stats AS (
        SELECT 
            year_group,
            COUNT(*) as total_rows,
            COUNT(CASE WHEN src_id IN (SELECT matched_src_row_id FROM matched_ids) THEN 1 END) as matched_count
        FROM src_with_year
        GROUP BY 1
    )
    SELECT 
        CAST(year_group AS INTEGER) as year,
        total_rows,
        matched_count,
        ROUND(CAST(matched_count AS DOUBLE) / total_rows * 100, 2) as match_rate
    FROM src_stats
    ORDER BY year
    """).df()

    logger.info("=" * 60)
    logger.info("MATCH RATES BY YEAR (Planilha Perspective)")
    logger.info("=" * 60)
    logger.info(f"{'Year':<6} | {'Total':>8} | {'Matched':>8} | {'Rate':>7}")
    logger.info("-" * 40)
    
    for _, row in stats_by_year.iterrows():
        year_display = str(int(row['year'])) if pd.notnull(row['year']) else "Unknown"
        logger.info(f"{year_display:<6} | {int(row['total_rows']):>8,} | {int(row['matched_count']):>8,} | {row['match_rate']:>6.2f}%")
    logger.info("=" * 60)

def main():
    logger.info("=== STARTING WATERFALL JOIN FOR EMBRYOSCOPE DATA ===")
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
