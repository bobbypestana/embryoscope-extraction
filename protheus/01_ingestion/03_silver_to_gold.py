#!/usr/bin/env python3
"""
Protheus Silver to Gold Consolidation Script
Combines silver.notas, silver.clientes, silver.produtos, silver.vendedores, and silver.tes
to build gold.protheus_mesclada_vendas. Maps customer and patient identifiers directly
from Protheus ERP tables, while resolving the couple chart ID (prontuario) via tiered Clinisys matching.
"""

import os
import yaml
import logging
import duckdb
from datetime import datetime

# Setup logging standard
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOGS_DIR = os.path.join(SCRIPT_DIR, 'logs')
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

# Load parameters
PARAMS_PATH = os.path.join(SCRIPT_DIR, 'params.yml')
with open(PARAMS_PATH, 'r') as f:
    config = yaml.safe_load(f)

DUCKDB_PATH = config['duckdb_path']
CLINISYS_DB_PATH = config['clinisys_db_path']
API_CONF = config['api']


def create_gold_table(con):
    logger.info("Combining Silver tables into gold.protheus_mesclada_vendas...")

    con.execute("DROP TABLE IF EXISTS gold.protheus_mesclada_vendas")

    query = """
    CREATE TABLE gold.protheus_mesclada_vendas AS
    SELECT
        TRY_CAST(c_cli.A1_CODMS AS INTEGER) AS "Cliente",
        COALESCE(c_cli.A1_NOME, n.F2_NOMPACI) AS "Nome",
        TRY_CAST(c_pac.A1_CODMS AS INTEGER)::VARCHAR AS "Paciente",
        n.F2_NOMPACI AS "Nom Paciente",
        TRY_CAST(-1 AS INTEGER) AS prontuario,
        CAST(n.F2_EMISSAO AS TIMESTAMP) AS "DT Emissao",
        p.B1_DESC AS "Descricao",
        n.D2_QUANT AS "Qntd.",
        n.D2_TOTAL AS "Total",
        CAST(NULL AS VARCHAR) AS "Descrição Gerencial",
        n.F2_FILIAL AS "Loja",
        n.D2_TES AS "Tipo da nota",
        TRY_CAST(n.F2_DOC AS INTEGER) AS "Numero",
        n.F2_SERIE AS "Serie Docto.",
        n.F2_NFELETR AS "NF Eletr.",
        n.F2_VEND1 AS "Vend. 1",
        v.A3_NOME AS "Médico",
        n.F2_CLIENTE AS "Cliente_totvs",
        CAST(NULL AS VARCHAR) AS "Operador",
        n.D2_COD AS "Produto",
        0.0 AS "Valor Mercadoria",
        0.0 AS "Custo",
        0.0 AS "Custo Unit",
        n.D2_DESC AS "Desconto",
        CASE
            -- Company 01 (Ibirapuera / Vila Mariana)
            WHEN n.company_id = '01' AND n.F2_FILIAL IN ('010101', '010150') THEN 'Ibirapuera'
            WHEN n.company_id = '01' AND n.F2_FILIAL IN ('010155', '010104', '010106') THEN 'Vila Mariana'
            -- Company 03 (Campinas)
            WHEN n.company_id = '03' AND n.F2_FILIAL = '030101' THEN 'Campinas'
            -- Company 06 (Pro Fiv / Santa Joana)
            WHEN n.company_id = '06' AND n.F2_FILIAL = '060101' THEN 'Pro Fiv'
            -- Company 05 (Belo Horizonte)
            WHEN n.company_id = '05' AND n.F2_FILIAL = '0101' THEN 'Belo Horizonte'
            -- Company 07 (Salvador - Cenafert / FIV Brasilia)
            WHEN n.company_id = '07' AND n.F2_FILIAL IN ('010101', '020101') THEN 'Salvador - Cenafert'
            WHEN n.company_id = '07' AND n.F2_FILIAL IN ('030101') THEN 'FIV Brasilia'
            ELSE 'Unknown Unit (' || COALESCE(n.company_id, '') || ', ' || COALESCE(n.F2_FILIAL, '') || ')'
        END AS "Unidade",
        MONTH(n.F2_EMISSAO) AS "Mês",
        YEAR(n.F2_EMISSAO) AS "Ano",
        n.D2_CONTA AS "Cta-Ctbl",
        CAST(NULL AS VARCHAR) AS "Cta-Ctbl Eugin",
        CAST(NULL AS VARCHAR) AS "Interno/Externo",
        CAST(NULL AS VARCHAR) AS "Descrição Mapping Actividad",
        0 AS "Ciclos",
        0 AS "Qnt Cons.",
        CASE 
            WHEN n.company_id = '01' THEN '1'
            WHEN n.company_id = '03' THEN '3'
            WHEN n.company_id = '05' THEN '5'
            WHEN n.company_id = '06' THEN '6'
            WHEN n.company_id = '07' THEN '7'
            ELSE 'Unknown'
        END AS "Grp",
        t.F4_TEXTO AS "Descr.TES",
        CAST(NULL AS VARCHAR) AS "Lead Time",
        CAST(NULL AS TIMESTAMP) AS "Data do Ciclo",
        'False' AS "Fez Ciclo?",
        ROW_NUMBER() OVER (ORDER BY n.F2_EMISSAO DESC, n.F2_DOC DESC, n.D2_ITEM ASC) AS line_number,
        n.extraction_timestamp AS extraction_timestamp,
        'Protheus API' AS file_name
    FROM silver.notas n
    LEFT JOIN silver.clientes c_cli
        ON n.F2_CLIENTE = c_cli.A1_COD AND n.F2_LOJA = c_cli.A1_LOJA
    LEFT JOIN silver.clientes c_pac
        ON n.F2_PACIENT = c_pac.A1_COD AND c_pac.A1_LOJA = '01'
    LEFT JOIN silver.produtos p
        ON n.D2_COD = p.B1_COD
    LEFT JOIN silver.vendedores v
        ON n.F2_VEND1 = v.A3_COD
    LEFT JOIN silver.tes t
        ON n.D2_TES = t.F4_CODIGO
    WHERE n.is_deleted = FALSE;
    """

    con.execute(query)
    count = con.execute("SELECT COUNT(*) FROM gold.protheus_mesclada_vendas").fetchone()[0]
    logger.info(f"Created gold.protheus_mesclada_vendas with {count:,} rows")


def update_prontuario_column(con):
    logger.info("Updating prontuario column using tiered matching logic...")

    logger.info(f"Attaching clinisys database from: {CLINISYS_DB_PATH}")
    con.execute(f"ATTACH '{CLINISYS_DB_PATH}' AS clinisys_all (READ_ONLY)")
    logger.info("clinisys_all database attached successfully")

    # Create temporary table with pre-cast/string-converted Clinisys identifiers to avoid cast failures
    con.execute("""
    CREATE TEMP TABLE temp_clinisys_processed AS
    SELECT
        codigo,
        codigo::VARCHAR AS codigo_str,
        TRY_CAST(prontuario_esposa AS INTEGER) AS prontuario_esposa,
        TRY_CAST(prontuario_marido AS INTEGER) AS prontuario_marido,
        TRY_CAST(prontuario_responsavel1 AS INTEGER) AS prontuario_responsavel1,
        TRY_CAST(prontuario_responsavel2 AS INTEGER) AS prontuario_responsavel2,
        
        regexp_replace(esposa_cpf, '[^0-9]', '', 'g') AS clean_cpf_esposa,
        regexp_replace(marido_cpf, '[^0-9]', '', 'g') AS clean_cpf_marido,
        
        strip_accents(LOWER(SPLIT_PART(TRIM(esposa_nome), ' ', 1))) as esposa_first,
        strip_accents(LOWER(SPLIT_PART(TRIM(marido_nome), ' ', 1))) as marido_first,
        esposa_nome,
        marido_nome,
        inativo
    FROM clinisys_all.silver.view_pacientes
    """)

    # 1. Tier 1: Direct Medsof ID matching (A1_CODMS from silver.clientes)
    logger.info("Tier 1: Matching prontuario via Medsof IDs (A1_CODMS)...")
    match_medsof_sql = """
    WITH raw_medsof AS (
        SELECT 
            g.line_number,
            TRY_CAST(c_cli.A1_CODMS AS INTEGER) AS cli_codms,
            TRY_CAST(c_pac.A1_CODMS AS INTEGER) AS pac_codms
        FROM gold.protheus_mesclada_vendas g
        LEFT JOIN silver.clientes c_cli ON g.Cliente_totvs = c_cli.A1_COD AND g.Loja = c_cli.A1_LOJA
        LEFT JOIN silver.clientes c_pac ON g.Paciente = c_pac.A1_COD AND c_pac.A1_LOJA = '01'
    ),
    medsof_matches AS (
        SELECT DISTINCT
            rm.line_number,
            COALESCE(vp1.codigo, vp2.codigo, vp3.codigo, vp4.codigo) as resolved_prontuario
        FROM raw_medsof rm
        LEFT JOIN temp_clinisys_processed vp1 ON rm.pac_codms = vp1.prontuario_esposa
        LEFT JOIN temp_clinisys_processed vp2 ON rm.pac_codms = vp2.prontuario_marido
        LEFT JOIN temp_clinisys_processed vp3 ON rm.cli_codms = vp3.prontuario_esposa
        LEFT JOIN temp_clinisys_processed vp4 ON rm.cli_codms = vp4.prontuario_marido
        WHERE COALESCE(vp1.codigo, vp2.codigo, vp3.codigo, vp4.codigo) IS NOT NULL
    )
    UPDATE gold.protheus_mesclada_vendas
    SET prontuario = m.resolved_prontuario
    FROM medsof_matches m
    WHERE gold.protheus_mesclada_vendas.line_number = m.line_number
    """
    con.execute(match_medsof_sql)
    
    stats_medsof = con.execute("SELECT COUNT(*), COUNT(CASE WHEN prontuario != -1 THEN 1 END) FROM gold.protheus_mesclada_vendas").fetchone()
    logger.info(f"Stats after Tier 1 (Medsof ID): Total={stats_medsof[0]:,}, Matched={stats_medsof[1]:,}, Rate={stats_medsof[1]/stats_medsof[0]*100:.2f}%")

    # 2. Tier 2: Match using CPF
    logger.info("Tier 2: Matching prontuario via CPF...")
    match_cpf_sql = """
    WITH raw_cpf AS (
        SELECT 
            g.line_number,
            regexp_replace(c_cli.A1_CGC, '[^0-9]', '', 'g') AS cli_cpf,
            regexp_replace(c_pac.A1_CGC, '[^0-9]', '', 'g') AS pac_cpf
        FROM gold.protheus_mesclada_vendas g
        LEFT JOIN silver.clientes c_cli ON g.Cliente_totvs = c_cli.A1_COD AND g.Loja = c_cli.A1_LOJA
        LEFT JOIN silver.clientes c_pac ON g.Paciente = c_pac.A1_COD AND c_pac.A1_LOJA = '01'
        WHERE g.prontuario = -1
    ),
    cpf_matches AS (
        SELECT DISTINCT
            rc.line_number,
            COALESCE(vp1.codigo, vp2.codigo, vp3.codigo, vp4.codigo) as resolved_prontuario
        FROM raw_cpf rc
        LEFT JOIN temp_clinisys_processed vp1 ON rc.pac_cpf = vp1.clean_cpf_esposa AND rc.pac_cpf != ''
        LEFT JOIN temp_clinisys_processed vp2 ON rc.pac_cpf = vp2.clean_cpf_marido AND rc.pac_cpf != ''
        LEFT JOIN temp_clinisys_processed vp3 ON rc.cli_cpf = vp3.clean_cpf_esposa AND rc.cli_cpf != ''
        LEFT JOIN temp_clinisys_processed vp4 ON rc.cli_cpf = vp4.clean_cpf_marido AND rc.cli_cpf != ''
        WHERE COALESCE(vp1.codigo, vp2.codigo, vp3.codigo, vp4.codigo) IS NOT NULL
    )
    UPDATE gold.protheus_mesclada_vendas
    SET prontuario = m.resolved_prontuario
    FROM cpf_matches m
    WHERE gold.protheus_mesclada_vendas.line_number = m.line_number
    """
    con.execute(match_cpf_sql)
    
    stats_cpf = con.execute("SELECT COUNT(*), COUNT(CASE WHEN prontuario != -1 THEN 1 END) FROM gold.protheus_mesclada_vendas").fetchone()
    logger.info(f"Stats after Tier 2 (CPF): Total={stats_cpf[0]:,}, Matched={stats_cpf[1]:,}, Rate={stats_cpf[1]/stats_cpf[0]*100:.2f}%")

    # 3. Tier 3: Match using Name matching (First names)
    logger.info("Tier 3: Matching prontuario via first names...")
    def match_names(name_column, is_inactive):
        inactive_val = '1' if is_inactive else '0'
        update_sql = f"""
        WITH mesclada_extract AS (
            SELECT DISTINCT
                "{name_column}" as name_val,
                strip_accents(LOWER(SPLIT_PART(TRIM("{name_column}"), ' ', 1))) as name_first
            FROM gold.protheus_mesclada_vendas
            WHERE prontuario = -1 AND "{name_column}" IS NOT NULL AND "{name_column}" != ''
        ),
        matches_esposa AS (
            SELECT d.*, p.codigo as resolved_prontuario, p.esposa_nome, p.marido_nome, 'esposa' as match_type
            FROM mesclada_extract d 
            INNER JOIN temp_clinisys_processed p ON d.name_first = p.esposa_first
            WHERE p.inativo = '{inactive_val}'
        ),
        matches_marido AS (
            SELECT d.*, p.codigo as resolved_prontuario, p.esposa_nome, p.marido_nome, 'marido' as match_type
            FROM mesclada_extract d 
            INNER JOIN temp_clinisys_processed p ON d.name_first = p.marido_first
            WHERE p.inativo = '{inactive_val}'
        ),
        all_matches AS (
            SELECT * FROM matches_esposa UNION SELECT * FROM matches_marido
        ),
        scored_matches AS (
            SELECT *,
                   CASE
                       WHEN match_type = 'esposa' AND strip_accents(LOWER(TRIM(name_val))) = strip_accents(LOWER(TRIM(esposa_nome))) THEN 0
                       WHEN match_type = 'marido' AND strip_accents(LOWER(TRIM(name_val))) = strip_accents(LOWER(TRIM(marido_nome))) THEN 0
                       WHEN match_type = 'esposa' THEN 2
                       ELSE 4
                   END as score
            FROM all_matches
        ),
        best_matches AS (
            SELECT name_val, resolved_prontuario FROM (
                SELECT name_val, resolved_prontuario, ROW_NUMBER() OVER (PARTITION BY name_val ORDER BY score ASC) as rn
                FROM scored_matches
            ) WHERE rn = 1
        )
        UPDATE gold.protheus_mesclada_vendas
        SET prontuario = bm.resolved_prontuario
        FROM best_matches bm
        WHERE gold.protheus_mesclada_vendas."{name_column}" = bm.name_val
          AND gold.protheus_mesclada_vendas.prontuario = -1;
        """
        con.execute(update_sql)

    logger.info("Matching Nom Paciente (active)...")
    match_names("Nom Paciente", is_inactive=False)
    logger.info("Matching Nome (active)...")
    match_names("Nome", is_inactive=False)
    logger.info("Matching Nom Paciente (inactive)...")
    match_names("Nom Paciente", is_inactive=True)
    logger.info("Matching Nome (inactive)...")
    match_names("Nome", is_inactive=True)

    stats_final = con.execute("SELECT COUNT(*), COUNT(CASE WHEN prontuario != -1 THEN 1 END) FROM gold.protheus_mesclada_vendas").fetchone()
    logger.info(f"Final prontuario matching stats: Total={stats_final[0]:,}, Matched={stats_final[1]:,}, Rate={stats_final[1]/stats_final[0]*100:.2f}%")

    con.execute("DROP TABLE temp_clinisys_processed")
    con.execute("DETACH clinisys_all")
    logger.info("Detached clinisys_all")


def main():
    logger.info("=== PROTHEUS SILVER TO GOLD CONSOLIDATION STARTED ===")
    logger.info(f"Target Database: {DUCKDB_PATH}")

    con = duckdb.connect(DUCKDB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")

    try:
        create_gold_table(con)
        # update_prontuario_column(con)
        logger.info("=== PROTHEUS SILVER TO GOLD CONSOLIDATION FINISHED SUCCESSFUL ===")
    except Exception as e:
        logger.error(f"Gold Consolidation Failed: {e}", exc_info=True)
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
