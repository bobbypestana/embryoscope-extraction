#!/usr/bin/env python3
"""
Protheus Silver to Gold Consolidation Script
Combines silver.notas, silver.clientes, silver.produtos, silver.vendedores, and silver.tes
to reproduce silver.mesclada_vendas. Implements the full validated 15-match-type Clinisys
patient matching logic (identical to finops/01_data_ingestion/03_02_mesclada_to_silver.py).
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

    query = f"""
    CREATE TABLE gold.protheus_mesclada_vendas AS
    SELECT
        TRY_CAST(n.F2_CLIENTE AS INTEGER) AS "Cliente",
        c.A1_NOME AS "Nome",
        n.F2_PACIENT AS "Paciente",
        n.F2_NOMPACI AS "Nom Paciente",
        CAST(-1 AS INTEGER) AS prontuario,
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
    LEFT JOIN silver.clientes c
        ON n.F2_CLIENTE = c.A1_COD AND n.F2_LOJA = c.A1_LOJA
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
    logger.info("Updating prontuario column using full 15-match-type Clinisys matching logic...")

    logger.info(f"Attaching clinisys database from: {CLINISYS_DB_PATH}")
    con.execute(f"ATTACH '{CLINISYS_DB_PATH}' AS clinisys_all")
    logger.info("clinisys_all database attached successfully")

    # 1. Match using Patient CPF
    logger.info("Matching using Patient CPF...")
    match_pat_cpf_sql = """
    WITH clean_clientes AS (
        SELECT DISTINCT
            A1_COD,
            regexp_replace(A1_CGC, '[^0-9]', '', 'g') as clean_cpf
        FROM silver.clientes
        WHERE A1_CGC IS NOT NULL AND A1_CGC != ''
    ),
    clean_clinisys AS (
        SELECT 
            codigo,
            regexp_replace(esposa_cpf, '[^0-9]', '', 'g') as clean_cpf_esposa,
            regexp_replace(marido_cpf, '[^0-9]', '', 'g') as clean_cpf_marido
        FROM clinisys_all.silver.view_pacientes
    ),
    matches AS (
        SELECT DISTINCT
            g.line_number,
            COALESCE(cl1.codigo, cl2.codigo) as resolved_prontuario
        FROM gold.protheus_mesclada_vendas g
        LEFT JOIN clean_clientes c_pat ON g.Paciente = c_pat.A1_COD
        LEFT JOIN clean_clinisys cl1 ON c_pat.clean_cpf = cl1.clean_cpf_esposa AND c_pat.clean_cpf != ''
        LEFT JOIN clean_clinisys cl2 ON c_pat.clean_cpf = cl2.clean_cpf_marido AND c_pat.clean_cpf != ''
        WHERE COALESCE(cl1.codigo, cl2.codigo) IS NOT NULL
    )
    UPDATE gold.protheus_mesclada_vendas
    SET prontuario = m.resolved_prontuario
    FROM matches m
    WHERE gold.protheus_mesclada_vendas.line_number = m.line_number
    """
    con.execute(match_pat_cpf_sql)

    # 2. Match using Client CPF
    logger.info("Matching using Client CPF...")
    match_cli_cpf_sql = """
    WITH clean_clientes AS (
        SELECT DISTINCT
            A1_COD,
            regexp_replace(A1_CGC, '[^0-9]', '', 'g') as clean_cpf
        FROM silver.clientes
        WHERE A1_CGC IS NOT NULL AND A1_CGC != ''
    ),
    clean_clinisys AS (
        SELECT 
            codigo,
            regexp_replace(esposa_cpf, '[^0-9]', '', 'g') as clean_cpf_esposa,
            regexp_replace(marido_cpf, '[^0-9]', '', 'g') as clean_cpf_marido
        FROM clinisys_all.silver.view_pacientes
    ),
    matches AS (
        SELECT DISTINCT
            g.line_number,
            COALESCE(cl1.codigo, cl2.codigo) as resolved_prontuario
        FROM gold.protheus_mesclada_vendas g
        LEFT JOIN clean_clientes c_cli ON g.Cliente_totvs = c_cli.A1_COD
        LEFT JOIN clean_clinisys cl1 ON c_cli.clean_cpf = cl1.clean_cpf_esposa AND c_cli.clean_cpf != ''
        LEFT JOIN clean_clinisys cl2 ON c_cli.clean_cpf = cl2.clean_cpf_marido AND c_cli.clean_cpf != ''
        WHERE g.prontuario = -1 AND COALESCE(cl1.codigo, cl2.codigo) IS NOT NULL
    )
    UPDATE gold.protheus_mesclada_vendas
    SET prontuario = m.resolved_prontuario
    FROM matches m
    WHERE gold.protheus_mesclada_vendas.line_number = m.line_number
    """
    con.execute(match_cli_cpf_sql)

    # Log stats after CPF matching
    stats_cpf = con.execute("SELECT COUNT(*), COUNT(CASE WHEN prontuario != -1 THEN 1 END) FROM gold.protheus_mesclada_vendas").fetchone()
    logger.info(f"Stats after CPF matching: Total={stats_cpf[0]:,}, Matched={stats_cpf[1]:,}, Rate={stats_cpf[1]/stats_cpf[0]*100:.2f}%")

    # Matching steps in priority order — each step only processes rows still unmatched (prontuario = -1)
    matching_steps = [
        {
            "name": "Paciente",
            "column": "Paciente",
            "name_field": "Nom Paciente",
            "filter_condition": 'AND "Paciente" IS NOT NULL AND "Paciente" != \'\'',
            "numeric_filter": False
        },
        {
            "name": "Cliente",
            "column": "Cliente",
            "name_field": "Nome",
            "filter_condition": 'AND "Cliente" IS NOT NULL',
            "numeric_filter": False
        },
        {
            "name": "Cliente_totvs",
            "column": "Cliente_totvs",
            "name_field": "Nom Paciente",
            "filter_condition": 'AND "Cliente_totvs" IS NOT NULL AND "Cliente_totvs" != \'\' AND TRY_CAST("Cliente_totvs" AS INTEGER) IS NOT NULL',
            "numeric_filter": True
        }
    ]

    logger.info("=== FIRST PASS: Active patients only (inativo = 0) ===")
    for i, step in enumerate(matching_steps, 1):
        logger.info(f"Step {i}: Matching using {step['name']} (active patients)...")
        update_prontuario_with_column(con, step, include_inactive=False)

    logger.info("=== SECOND PASS: Inactive patients only (inativo = 1) ===")
    for i, step in enumerate(matching_steps, 1):
        logger.info(f"Step {i}: Matching using {step['name']} (inactive patients)...")
        update_prontuario_with_column(con, step, include_inactive=True)

    con.execute("DETACH clinisys_all")
    logger.info("Detached clinisys_all")


def update_prontuario_with_column(con, step_config, include_inactive=False):
    """
    Full 15-match-type prontuario matching algorithm, identical to the validated
    implementation in finops/01_data_ingestion/03_02_mesclada_to_silver.py.
    Searches all cross-clinic prontuario fields (esposa/marido/responsavel across
    PEL, PC, FC, BA clinics) and scores by both name quality and match type priority.
    """
    column_name = step_config["name"]
    column_field = step_config["column"]
    name_field = step_config["name_field"]
    filter_condition = step_config["filter_condition"]

    inactive_condition = "inativo = 1" if include_inactive else "inativo = 0"
    match_type_prefix = column_name.lower()

    update_sql = f"""
    WITH
    -- CTE 1: Extract distinct unmatched source keys + first-word name normalization
    mesclada_extract AS (
        SELECT DISTINCT
            "{column_field}" as {column_field},
            CASE WHEN "Nome" IS NOT NULL
                 THEN strip_accents(LOWER(SPLIT_PART(TRIM("Nome"), ' ', 1)))
                 ELSE NULL END as nome_first,
            CASE WHEN "Nom Paciente" IS NOT NULL
                 THEN strip_accents(LOWER(SPLIT_PART(TRIM("Nom Paciente"), ' ', 1)))
                 ELSE NULL END as nom_paciente_first
        FROM gold.protheus_mesclada_vendas
        WHERE prontuario = -1
          {filter_condition}
    ),

    -- CTE 1B: Pre-process Clinisys data — normalize names, pull all prontuario ID fields
    clinisys_processed AS (
        SELECT
            codigo,
            prontuario_esposa,
            prontuario_marido,
            prontuario_responsavel1,
            prontuario_responsavel2,
            prontuario_esposa_pel,
            prontuario_marido_pel,
            prontuario_esposa_pc,
            prontuario_marido_pc,
            prontuario_responsavel1_pc,
            prontuario_responsavel2_pc,
            prontuario_esposa_fc,
            prontuario_marido_fc,
            prontuario_esposa_ba,
            prontuario_marido_ba,
            strip_accents(LOWER(SPLIT_PART(TRIM(esposa_nome), ' ', 1))) as esposa_nome,
            strip_accents(LOWER(SPLIT_PART(TRIM(marido_nome), ' ', 1))) as marido_nome,
            unidade_origem
        FROM clinisys_all.silver.view_pacientes
        WHERE {inactive_condition}
    ),

    -- CTEs 2–16: One join per prontuario field type
    matches_1 AS (
        SELECT d.*, p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               '{match_type_prefix}_main' as match_type
        FROM mesclada_extract d INNER JOIN clinisys_processed p ON d.{column_field} = p.codigo
    ),
    matches_2 AS (
        SELECT d.*, p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               '{match_type_prefix}_esposa' as match_type
        FROM mesclada_extract d INNER JOIN clinisys_processed p ON d.{column_field} = p.prontuario_esposa
    ),
    matches_3 AS (
        SELECT d.*, p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               '{match_type_prefix}_marido' as match_type
        FROM mesclada_extract d INNER JOIN clinisys_processed p ON d.{column_field} = p.prontuario_marido
    ),
    matches_4 AS (
        SELECT d.*, p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               '{match_type_prefix}_responsavel1' as match_type
        FROM mesclada_extract d INNER JOIN clinisys_processed p ON d.{column_field} = p.prontuario_responsavel1
    ),
    matches_5 AS (
        SELECT d.*, p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               '{match_type_prefix}_responsavel2' as match_type
        FROM mesclada_extract d INNER JOIN clinisys_processed p ON d.{column_field} = p.prontuario_responsavel2
    ),
    matches_6 AS (
        SELECT d.*, p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               '{match_type_prefix}_esposa_pel' as match_type
        FROM mesclada_extract d INNER JOIN clinisys_processed p ON d.{column_field} = p.prontuario_esposa_pel
    ),
    matches_7 AS (
        SELECT d.*, p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               '{match_type_prefix}_marido_pel' as match_type
        FROM mesclada_extract d INNER JOIN clinisys_processed p ON d.{column_field} = p.prontuario_marido_pel
    ),
    matches_8 AS (
        SELECT d.*, p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               '{match_type_prefix}_esposa_pc' as match_type
        FROM mesclada_extract d INNER JOIN clinisys_processed p ON d.{column_field} = p.prontuario_esposa_pc
    ),
    matches_9 AS (
        SELECT d.*, p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               '{match_type_prefix}_marido_pc' as match_type
        FROM mesclada_extract d INNER JOIN clinisys_processed p ON d.{column_field} = p.prontuario_marido_pc
    ),
    matches_10 AS (
        SELECT d.*, p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               '{match_type_prefix}_responsavel1_pc' as match_type
        FROM mesclada_extract d INNER JOIN clinisys_processed p ON d.{column_field} = p.prontuario_responsavel1_pc
    ),
    matches_11 AS (
        SELECT d.*, p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               '{match_type_prefix}_responsavel2_pc' as match_type
        FROM mesclada_extract d INNER JOIN clinisys_processed p ON d.{column_field} = p.prontuario_responsavel2_pc
    ),
    matches_12 AS (
        SELECT d.*, p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               '{match_type_prefix}_esposa_fc' as match_type
        FROM mesclada_extract d INNER JOIN clinisys_processed p ON d.{column_field} = p.prontuario_esposa_fc
    ),
    matches_13 AS (
        SELECT d.*, p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               '{match_type_prefix}_marido_fc' as match_type
        FROM mesclada_extract d INNER JOIN clinisys_processed p ON d.{column_field} = p.prontuario_marido_fc
    ),
    matches_14 AS (
        SELECT d.*, p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               '{match_type_prefix}_esposa_ba' as match_type
        FROM mesclada_extract d INNER JOIN clinisys_processed p ON d.{column_field} = p.prontuario_esposa_ba
    ),
    matches_15 AS (
        SELECT d.*, p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               '{match_type_prefix}_marido_ba' as match_type
        FROM mesclada_extract d INNER JOIN clinisys_processed p ON d.{column_field} = p.prontuario_marido_ba
    ),

    -- CTE 17: Union all match types
    all_matches AS (
        SELECT * FROM matches_1  UNION SELECT * FROM matches_2  UNION SELECT * FROM matches_3
        UNION SELECT * FROM matches_4  UNION SELECT * FROM matches_5  UNION SELECT * FROM matches_6
        UNION SELECT * FROM matches_7  UNION SELECT * FROM matches_8  UNION SELECT * FROM matches_9
        UNION SELECT * FROM matches_10 UNION SELECT * FROM matches_11 UNION SELECT * FROM matches_12
        UNION SELECT * FROM matches_13 UNION SELECT * FROM matches_14 UNION SELECT * FROM matches_15
    ),

    -- CTE 18: Score by name quality + match type priority (lower = better)
    scored_matches AS (
        SELECT *,
               CASE
                   WHEN (nome_first = esposa_nome AND nom_paciente_first = marido_nome)
                        OR (nom_paciente_first = esposa_nome AND nome_first = marido_nome) THEN 0
                   WHEN (nome_first = esposa_nome OR nom_paciente_first = marido_nome)
                        OR (nom_paciente_first = esposa_nome OR nome_first = marido_nome) THEN 2
                   ELSE 4
               END as name_match_score,
               CASE
                   WHEN match_type = '{match_type_prefix}_main'             THEN 1
                   WHEN match_type = '{match_type_prefix}_esposa'           THEN 3
                   WHEN match_type = '{match_type_prefix}_marido'           THEN 5
                   WHEN match_type = '{match_type_prefix}_responsavel1'     THEN 7
                   WHEN match_type = '{match_type_prefix}_responsavel2'     THEN 9
                   WHEN match_type = '{match_type_prefix}_esposa_pel'       THEN 11
                   WHEN match_type = '{match_type_prefix}_marido_pel'       THEN 13
                   WHEN match_type = '{match_type_prefix}_esposa_pc'        THEN 15
                   WHEN match_type = '{match_type_prefix}_marido_pc'        THEN 17
                   WHEN match_type = '{match_type_prefix}_responsavel1_pc'  THEN 19
                   WHEN match_type = '{match_type_prefix}_responsavel2_pc'  THEN 21
                   WHEN match_type = '{match_type_prefix}_esposa_fc'        THEN 23
                   WHEN match_type = '{match_type_prefix}_marido_fc'        THEN 25
                   WHEN match_type = '{match_type_prefix}_esposa_ba'        THEN 27
                   WHEN match_type = '{match_type_prefix}_marido_ba'        THEN 29
                   ELSE 31
               END as match_type_score
        FROM all_matches
        WHERE nome_first = esposa_nome OR nome_first = marido_nome
           OR nom_paciente_first = esposa_nome OR nom_paciente_first = marido_nome
    ),

    -- CTE 19: Rank by combined score
    ranked_matches AS (
        SELECT *,
               (name_match_score + match_type_score) as combined_score,
               ROW_NUMBER() OVER (
                   PARTITION BY {column_field}, "prontuario"
                   ORDER BY (name_match_score + match_type_score)
               ) as rn
        FROM scored_matches
    ),

    -- CTE 20: Best match per source key
    best_matches AS (
        SELECT * FROM ranked_matches rm1
        WHERE rn = (
            SELECT MIN(rn) FROM ranked_matches rm2
            WHERE rm2.{column_field} = rm1.{column_field}
        )
    )

    UPDATE gold.protheus_mesclada_vendas
    SET prontuario = COALESCE(bm.prontuario, -1)
    FROM best_matches bm
    WHERE gold.protheus_mesclada_vendas."{column_field}" = bm.{column_field}
      AND gold.protheus_mesclada_vendas.prontuario = -1
      AND (strip_accents(TRIM(LOWER(SPLIT_PART(gold.protheus_mesclada_vendas."{name_field}", ' ', 1)))) = bm.nome_first
        OR strip_accents(TRIM(LOWER(SPLIT_PART(gold.protheus_mesclada_vendas."{name_field}", ' ', 1)))) = bm.nom_paciente_first)
    """

    try:
        con.execute(update_sql)
        result = con.execute("""
            SELECT
                COUNT(*) as total_rows,
                COUNT(CASE WHEN prontuario != -1 THEN 1 END) as matched_rows,
                COUNT(CASE WHEN prontuario = -1 THEN 1 END) as unmatched_rows
            FROM gold.protheus_mesclada_vendas
        """).fetchone()
        label = "inactive" if include_inactive else "active"
        logger.info(f"Stats after {column_name} ({label}) matching:")
        logger.info(f"  Total rows:     {result[0]:,}")
        logger.info(f"  Matched rows:   {result[1]:,}")
        logger.info(f"  Unmatched rows: {result[2]:,}")
        logger.info(f"  Match rate:     {(result[1]/result[0]*100):.2f}%")
    except Exception as e:
        logger.error(f"Error in {column_name} matching: {e}")
        raise


def main():
    logger.info("=== PROTHEUS SILVER TO GOLD CONSOLIDATION STARTED ===")
    logger.info(f"Target Database: {DUCKDB_PATH}")

    grp_const = str(int(API_CONF['tenant_id'].split(',')[0]))
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
