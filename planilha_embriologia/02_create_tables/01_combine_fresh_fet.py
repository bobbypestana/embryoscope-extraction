#!/usr/bin/env python3
"""
01_combine_fresh_fet.py
Merges silver.planilha_embriologia_fresh (left) and silver.planilha_embriologia_fet (right)
Join conditions:
- prontuario matches
- Cryopreservation date matches within +/- 2 day tolerance
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
    """Combine FRESH and FET tables with 2-day tolerance join on cryo date"""
    
    logger.info("Merging FRESH and FET cycles...")
    
    # Drop existing table
    conn.execute("DROP TABLE IF EXISTS silver.planilha_embriologia_combined")
    
    # Selected columns from FRESH: prontuario, data_da_puncao, fator_1, incubadora, data_cryo
    # Selected columns from FET: prontuario, data_da_fet, data_crio, result (fet_resultado), tipo_do_resultado (fet_tipo_resultado), no_nascidos (fet_no_nascidos)
    
    query = """
    CREATE TABLE silver.planilha_embriologia_combined AS
    WITH fresh AS (
        SELECT *, ROW_NUMBER() OVER() as fresh_id
        FROM silver.planilha_embriologia_fresh
    ),
    fet AS (
        SELECT *, ROW_NUMBER() OVER() as fet_id
        FROM silver.planilha_embriologia_fet
    ),
    step1 AS (
        -- Step 1: Matching Prontuario + Crio Date (+/- 2 days)
        SELECT f.fresh_id, t.fet_id, 1 as step_id
        FROM fresh f
        JOIN fet t ON f.prontuario = t.prontuario
        WHERE date_diff('day', TRY_CAST(f.data_crio AS TIMESTAMP), TRY_CAST(t.data_crio AS TIMESTAMP)) BETWEEN -2 AND 2
          AND f.prontuario IS NOT NULL
    ),
    step2 AS (
        -- Step 2: Matching Prontuario + FET Date == Punção Date (+/- 2 days)
        SELECT f.fresh_id, t.fet_id, 2 as step_id
        FROM fresh f
        JOIN fet t ON f.prontuario = t.prontuario
        WHERE date_diff('day', TRY_CAST(f.data_da_puncao AS TIMESTAMP), TRY_CAST(t.data_da_fet AS TIMESTAMP)) BETWEEN -2 AND 2
          AND f.prontuario IS NOT NULL
          AND f.fresh_id NOT IN (SELECT fresh_id FROM step1)
          AND t.fet_id NOT IN (SELECT fet_id FROM step1)
    ),
    all_combined_matches AS (
        SELECT * FROM step1
        UNION ALL
        SELECT * FROM step2
    ),
    final_matches AS (
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY fresh_id ORDER BY step_id) as rn_fresh,
                      ROW_NUMBER() OVER(PARTITION BY fet_id ORDER BY step_id) as rn_fet
            FROM all_combined_matches
        ) WHERE rn_fresh = 1 AND rn_fet = 1
    )
    SELECT 
        COALESCE(CAST(f.prontuario AS INTEGER), CAST(t.prontuario AS INTEGER)) as prontuario,
        m.step_id as join_step,
        -- Fresh Columns
        CAST(f.data_da_puncao AS DATE) as fresh_data_da_puncao,
        f.fator_1 as fresh_fator_1,
        f.incubadora as fresh_incubadora,
        CAST(f.data_crio AS DATE) as fresh_data_crio,
        f.tipo_1 as fresh_tipo_1,
        f.tipo_de_inseminacao as fresh_tipo_de_inseminacao,
        f.tipo_biopsia as fresh_tipo_biopsia,
        f.altura as fresh_altura,
        f.peso as fresh_peso,
        f.data_de_nasc as fresh_data_de_nasc,
        f.idade_espermatozoide as fresh_idade_espermatozoide,
        f.origem as fresh_origem,
        f.tipo as fresh_tipo,
        f.opu as fresh_opu,
        f.total_de_mii as fresh_total_de_mii,
        f.qtd_blasto as fresh_qtd_blasto,
        f.qtd_blasto_tq_a_e_b as fresh_qtd_blasto_tq_a_e_b,
        f.no_biopsiados as fresh_no_biopsiados,
        f.qtd_analisados as fresh_qtd_analisados,
        f.qtd_normais as fresh_qtd_normais,
        f.dia_cryo as fresh_dia_cryo,
        f.file_name as fresh_file_name,
        f.sheet_name as fresh_sheet_name,
        -- FET Columns
        CAST(t.data_da_fet AS DATE) as fet_data_da_fet,
        CAST(t.data_crio AS DATE) as fet_data_crio,
        t.result as fet_resultado,
        t.tipo_do_resultado as fet_tipo_resultado,
        t.no_nascidos as fet_no_nascidos,
        t.tipo_1 as fet_tipo_1,
        t.tipo_de_tratamento as fet_tipo_de_tratamento,
        t.tipo_de_fet as fet_tipo_de_fet,
        t.tipo_biopsia as fet_tipo_biopsia,
        t.tipo_da_doacao as fet_tipo_da_doacao,
        t.idade_mulher as fet_idade_mulher,
        t.idade_do_cong_de_embriao as fet_idade_do_cong_de_embriao,
        t.preparo_para_transferencia as fet_preparo_para_transferencia,
        t.dia_cryo as fet_dia_cryo,
        t.no_da_transfer_1a_2a_3a as fet_no_da_transfer_1a_2a_3a,
        t.dia_et as fet_dia_et,
        t.no_et as fet_no_et,
        t.gravidez_bioquimica as fet_gravidez_bioquimica,
        t.gravidez_clinica as fet_gravidez_clinica,
        t.file_name as fet_file_name,
        t.sheet_name as fet_sheet_name
    FROM fresh f
    LEFT JOIN final_matches m ON f.fresh_id = m.fresh_id
    FULL OUTER JOIN fet t ON m.fet_id = t.fet_id
    """
    
    conn.execute(query)
    
    # Coverage Stats
    stats = conn.execute("""
        SELECT 
            (SELECT COUNT(*) FROM silver.planilha_embriologia_fresh) as total_fresh,
            (SELECT COUNT(*) FROM silver.planilha_embriologia_fet) as total_fet,
            COUNT(*) as total_combined_rows,
            COUNT(CASE WHEN join_step IS NOT NULL THEN 1 END) as both_sides,
            COUNT(CASE WHEN fresh_data_da_puncao IS NOT NULL AND join_step IS NULL THEN 1 END) as fresh_only,
            COUNT(CASE WHEN fet_data_da_fet IS NOT NULL AND join_step IS NULL THEN 1 END) as fet_only
        FROM silver.planilha_embriologia_combined
    """).df().iloc[0]
    
    stats_by_step = conn.execute("""
        SELECT join_step, COUNT(*) as count
        FROM silver.planilha_embriologia_combined
        WHERE join_step IS NOT NULL
        GROUP BY join_step
        ORDER BY join_step
    """).df()

    total_fresh = stats['total_fresh']
    total_fet = stats['total_fet']
    total_combined = stats['total_combined_rows']
    both_sides = stats['both_sides']
    fresh_only = stats['fresh_only']
    fet_only = stats['fet_only']
    
    # Calculate FET match rate
    fet_match_rate = (both_sides / total_fet * 100) if total_fet > 0 else 0
    
    logger.info("==================================================")
    logger.info("WATERFALL MERGE STATISTICS (FRESH <-> FET)")
    logger.info("==================================================")
    logger.info(f"Source FRESH rows: {total_fresh:,}")
    logger.info(f"Source FET rows:   {total_fet:,}")
    logger.info(f"--------------------------------------------------")
    logger.info(f"Total combined rows: {total_combined:,}")
    logger.info(f"  Rows with BOTH (matched): {both_sides:,} ({fet_match_rate:.2f}% of FET)")
    logger.info(f"  Rows with FRESH only:     {fresh_only:,}")
    logger.info(f"  Rows with FET only:       {fet_only:,}")
    logger.info("--------------------------------------------------")
    logger.info("Matches by Waterfall Step:")
    
    step_labels = {
        1: "Prontuario + Crio Date (+/- 2d)",
        2: "Prontuario + FET Date == Punção Date"
    }
    
    for _, row in stats_by_step.iterrows():
        step = int(row['join_step'])
        label = step_labels.get(step, f"Step {step}")
        logger.info(f"  Step {step}: {label:<35} | {int(row['count']):>6,}")
    
    logger.info("==================================================")
    logger.info("Result saved to silver.planilha_embriologia_combined")


def main():
    logger.info("=== STARTING FRESH-FET MERGER (2-DAY TOLERANCE) ===")
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
