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
    """Create the gold.planilha_embryoscope_combined table with waterfall logic"""
    
    logger.info("Executing Waterfall Join between Embryoscope and Redlara-Planilha...")
    
    # Drop existing table
    conn.execute("DROP TABLE IF EXISTS gold.planilha_embryoscope_combined")
    
    # We use a series of CTEs to identify matches at each level
    # Priority: Puncao -> Transfer (Descong, Trat1, Trat2) -> Cryo (Redlara, FET, Fresh)
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
        SELECT * 
        FROM gold.redlara_planilha_combined
    ),
    step1 AS (
        -- Step 1: Punção (micro_Data_DL) matches fresh_data_da_puncao (PRIORITY)
        SELECT e.oocito_id, s.prontuario as s_prontuario, s.transfer_date as s_transfer_date, 1 as step_id
        FROM emb e JOIN src s ON e.prontuario_int = s.prontuario AND e.puncao_date = s.fresh_data_da_puncao
        WHERE e.puncao_date IS NOT NULL AND s.fresh_data_da_puncao IS NOT NULL
    ),
    step2 AS (
        -- Step 2: descong_em_DataTransferencia = transfer_date
        SELECT e.oocito_id, s.prontuario as s_prontuario, s.transfer_date as s_transfer_date, 2 as step_id
        FROM emb e JOIN src s ON e.prontuario_int = s.prontuario AND e.transfer_date_descong = s.transfer_date
        WHERE e.transfer_date_descong IS NOT NULL AND s.transfer_date IS NOT NULL
          AND e.oocito_id NOT IN (SELECT oocito_id FROM step1)
    ),
    step3 AS (
        -- Step 3: trat1_data_transferencia = transfer_date
        SELECT e.oocito_id, s.prontuario as s_prontuario, s.transfer_date as s_transfer_date, 3 as step_id
        FROM emb e JOIN src s ON e.prontuario_int = s.prontuario AND e.transfer_date_trat1 = s.transfer_date
        WHERE e.transfer_date_trat1 IS NOT NULL AND s.transfer_date IS NOT NULL
          AND e.oocito_id NOT IN (SELECT oocito_id FROM step1 UNION SELECT oocito_id FROM step2)
    ),
    step4 AS (
        -- Step 4: trat2_data_transferencia = transfer_date
        SELECT e.oocito_id, s.prontuario as s_prontuario, s.transfer_date as s_transfer_date, 4 as step_id
        FROM emb e JOIN src s ON e.prontuario_int = s.prontuario AND e.transfer_date_trat2 = s.transfer_date
        WHERE e.transfer_date_trat2 IS NOT NULL AND s.transfer_date IS NOT NULL
          AND e.oocito_id NOT IN (SELECT oocito_id FROM step1 UNION SELECT oocito_id FROM step2 UNION SELECT oocito_id FROM step3)
    ),
    step5 AS (
        -- Step 5: cong_em_Data = date_when_embryos_were_cryopreserved
        SELECT e.oocito_id, s.prontuario as s_prontuario, s.transfer_date as s_transfer_date, 5 as step_id
        FROM emb e JOIN src s ON e.prontuario_int = s.prontuario AND e.cong_date = s.date_when_embryos_were_cryopreserved
        WHERE e.cong_date IS NOT NULL AND s.date_when_embryos_were_cryopreserved IS NOT NULL
          AND e.oocito_id NOT IN (SELECT oocito_id FROM step1 UNION SELECT oocito_id FROM step2 UNION SELECT oocito_id FROM step3 UNION SELECT oocito_id FROM step4)
    ),
    step6 AS (
        -- Step 6: cong_em_Data = fet_data_crio
        SELECT e.oocito_id, s.prontuario as s_prontuario, s.transfer_date as s_transfer_date, 6 as step_id
        FROM emb e JOIN src s ON e.prontuario_int = s.prontuario AND e.cong_date = s.fet_data_crio
        WHERE e.cong_date IS NOT NULL AND s.fet_data_crio IS NOT NULL
          AND e.oocito_id NOT IN (SELECT oocito_id FROM step1 UNION SELECT oocito_id FROM step2 UNION SELECT oocito_id FROM step3 UNION SELECT oocito_id FROM step4 UNION SELECT oocito_id FROM step5)
    ),
    step7 AS (
        -- Step 7: cong_em_Data = fresh_data_crio
        SELECT e.oocito_id, s.prontuario as s_prontuario, s.transfer_date as s_transfer_date, 7 as step_id
        FROM emb e JOIN src s ON e.prontuario_int = s.prontuario AND e.cong_date = s.fresh_data_crio
        WHERE e.cong_date IS NOT NULL AND s.fresh_data_crio IS NOT NULL
          AND e.oocito_id NOT IN (SELECT oocito_id FROM step1 UNION SELECT oocito_id FROM step2 UNION SELECT oocito_id FROM step3 UNION SELECT oocito_id FROM step4 UNION SELECT oocito_id FROM step5 UNION SELECT oocito_id FROM step6)
    ),
    all_matches AS (
        SELECT * FROM step1 UNION ALL SELECT * FROM step2 UNION ALL SELECT * FROM step3 UNION ALL SELECT * FROM step4 UNION ALL SELECT * FROM step5 UNION ALL SELECT * FROM step6 UNION ALL SELECT * FROM step7
    ),
    final_matches AS (
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY oocito_id ORDER BY step_id) as rn
            FROM all_matches
        ) WHERE rn = 1
    )
    SELECT 
        e.*,
        m.step_id as join_step,
        s.* EXCLUDE (prontuario, transfer_date, fresh_data_da_puncao, date_when_embryos_were_cryopreserved, fresh_data_crio, fet_data_crio)
    FROM gold.embryoscope_clinisys_combined e
    LEFT JOIN final_matches m ON e.oocito_id = m.oocito_id
    LEFT JOIN src s ON m.s_prontuario = s.prontuario AND m.s_transfer_date = s.transfer_date
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
    
    total = stats_overall['total_embryos']
    matched = stats_overall['matched_embryos']
    rate = (matched / total * 100) if total > 0 else 0
    
    logger.info("=" * 60)
    logger.info("WATERFALL JOIN SUMMARY (Embryoscope -> Planilha-Redlara)")
    logger.info("=" * 60)
    logger.info(f"Total Embryos in Embryoscope: {total:,}")
    logger.info(f"Matched with Planilha/Redlara: {matched:,} ({rate:.2f}%)")
    logger.info("-" * 60)
    logger.info("Matches by Waterfall Step:")
    
    step_labels = {
        1: "Punção (Date DL)",
        2: "Transfer (Descong)",
        3: "Transfer (Trat1)",
        4: "Transfer (Trat2)",
        5: "Cryo (Redlara)",
        6: "Cryo (FET)",
        7: "Cryo (Fresh)"
    }
    
    for _, row in stats_by_step.iterrows():
        step = int(row['join_step'])
        label = step_labels.get(step, f"Step {step}")
        logger.info(f"  Step {step}: {label:<25} | {int(row['count']):>6,}")
    
    logger.info("=" * 60)
    logger.info("Gold table gold.planilha_embryoscope_combined created.")

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
