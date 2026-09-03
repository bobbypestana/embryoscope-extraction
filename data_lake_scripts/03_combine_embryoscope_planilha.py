#!/usr/bin/env python3
"""
03_combine_embryoscope_planilha.py
Combines gold.clinisys_embrioes_outcomes (treatments + verified deduplicated outcomes) 
with gold.embryoscope_embrioes (morphokinetics) to build:
gold.pesquisa_embrioes_com_tratamento_morfocinetica_desfechos.

Strict Invariants:
- 1:1 grain on oocito_id (exactly 321,168 rows, 0 duplicates)
- Deduplicated Embryoscope match picking closest fertilization date to puncture
- Consolidated quality flags: has_biopsy and has_valid_outcome
- Backward-compatible column aliases for legacy queries
- No legacy views in production
"""

import duckdb as db
import pandas as pd
from datetime import datetime
import os
import logging
import time

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

def safe_drop(conn, name):
    for obj_type in ['VIEW', 'TABLE']:
        try:
            conn.execute(f"DROP {obj_type} IF EXISTS {name};")
        except Exception:
            pass

def create_combined_table(conn):
    """Create the gold.pesquisa_embrioes_com_tratamento_morfocinetica_desfechos table"""
    
    logger.info("Building gold.pesquisa_embrioes_com_tratamento_morfocinetica_desfechos...")
    t0 = time.time()
    
    # Safe drops
    safe_drop(conn, "gold.planilha_embryoscope_combined")
    safe_drop(conn, "gold.pesquisa_embrioes_com_tratamento_morfocinetica_desfechos")
    
    # Pre-match Embryoscope on oocito_id picking closest fertilization date to puncture
    conn.execute("""
    CREATE OR REPLACE TEMP TABLE tmp_clinisys_es_matches AS
    SELECT 
        c.oocito_id,
        e.embryo_EmbryoID,
        ROW_NUMBER() OVER (
            PARTITION BY c.oocito_id 
            ORDER BY ABS(DATEDIFF('day', CAST(e.embryo_FertilizationTime AS DATE), CAST(c.micro_Data_DL AS DATE))), e.embryo_EmbryoID
        ) as rn
    FROM gold.clinisys_embrioes_outcomes c
    JOIN gold.embryoscope_embrioes e
        ON c.micro_prontuario = e.prontuario
        AND c.oocito_embryo_number = e.embryo_embryo_number
        AND CAST(e.embryo_FertilizationTime AS DATE) BETWEEN (CAST(c.micro_Data_DL AS DATE) - 3) AND (CAST(c.micro_Data_DL AS DATE) + 3);
    """)
    
    # Create final table
    query = """
    CREATE TABLE gold.pesquisa_embrioes_com_tratamento_morfocinetica_desfechos AS
    SELECT 
        c.* EXCLUDE (has_biopsy, has_valid_outcome),
        
        -- Morphokinetics & Embryoscope fields
        e.* EXCLUDE (prontuario),
        
        -- Consolidated High-Level Quality & Modeling Flags
        (
            c.has_biopsy = True OR 
            (e.embryo_Description IS NOT NULL AND TRIM(e.embryo_Description) != '' AND LOWER(TRIM(e.embryo_Description)) != 'none')
        ) as has_biopsy,
        
        c.has_valid_outcome as has_valid_outcome,
        
        -- Backward-compatible column aliases for legacy queries
        c.outcome_final_result as outcome_type,
        c.outcome_final_gravidez_clinica as fet_gravidez_clinica,
        c.outcome_final_no_nascidos as merged_numero_de_nascidos,
        c.planilha_tipo_resultado as fet_tipo_resultado

    FROM gold.clinisys_embrioes_outcomes c
    LEFT JOIN (
        SELECT oocito_id, embryo_EmbryoID 
        FROM tmp_clinisys_es_matches 
        WHERE rn = 1
    ) m ON c.oocito_id = m.oocito_id
    LEFT JOIN gold.embryoscope_embrioes e ON m.embryo_EmbryoID = e.embryo_EmbryoID;
    """
    
    conn.execute(query)
    
    elapsed = time.time() - t0
    row_cnt = conn.execute("SELECT count(*) FROM gold.pesquisa_embrioes_com_tratamento_morfocinetica_desfechos").fetchone()[0]
    dist_id = conn.execute("SELECT count(distinct oocito_id) FROM gold.pesquisa_embrioes_com_tratamento_morfocinetica_desfechos").fetchone()[0]
    es_cnt = conn.execute("SELECT count(*) FROM gold.pesquisa_embrioes_com_tratamento_morfocinetica_desfechos WHERE embryo_EmbryoID IS NOT NULL").fetchone()[0]
    biop_cnt = conn.execute("SELECT count(*) FROM gold.pesquisa_embrioes_com_tratamento_morfocinetica_desfechos WHERE has_biopsy = True").fetchone()[0]
    valid_out = conn.execute("SELECT count(*) FROM gold.pesquisa_embrioes_com_tratamento_morfocinetica_desfechos WHERE has_valid_outcome = True").fetchone()[0]

    logger.info("=" * 80)
    logger.info("PESQUISA EMBRIOES COM TRATAMENTO MORFOCINETICA DESFECHOS SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Successfully built table in {elapsed:.2f}s!")
    logger.info(f"Total rows: {row_cnt:,d} | Distinct oocito_ids: {dist_id:,d} (Duplicates: {row_cnt - dist_id})")
    logger.info(f"Embryoscope matched rows: {es_cnt:,d}")
    logger.info(f"has_biopsy = True: {biop_cnt:,d} ({biop_cnt/row_cnt*100:.2f}%)")
    logger.info(f"has_valid_outcome = True: {valid_out:,d} ({valid_out/row_cnt*100:.2f}%)")
    logger.info("=" * 80)

def main():
    conn = get_database_connection(read_only=False)
    try:
        create_combined_table(conn)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
