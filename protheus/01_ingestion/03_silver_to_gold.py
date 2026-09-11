#!/usr/bin/env python3
"""
Protheus Silver to Gold Consolidation Script
Combines silver.notas, silver.clientes, silver.produtos, silver.vendedores, and silver.tes
to build gold.protheus_notas_faturadas and gold.protheus_pedidos_a_faturar.
Maps customer and patient identifiers directly from Protheus ERP tables, while resolving the couple chart ID (prontuario) via tiered Clinisys matching.
"""

import sys
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

# Add Huntington root directory to sys.path to resolve commons
_root_dir = os.path.dirname(os.path.dirname(SCRIPT_DIR))
if _root_dir not in sys.path:
    sys.path.insert(0, _root_dir)

from commons.prontuario_matching_v1 import find_prontuarios


def create_dim_paciente_crosswalk(con):
    logger.info("=== Creating gold.dim_paciente_crosswalk (Single-Pass Strategy L) ===")
    
    con.execute("""
    CREATE OR REPLACE TABLE gold._stage_dim_paciente_crosswalk AS
    WITH all_entities AS (
        SELECT 
            instance_id,
            COALESCE(NULLIF(TRIM(A1_CODMS), ''), NULLIF(TRIM(A1_COD), '')) as entity_id,
            NULLIF(TRIM(A1_NOME), '') as entity_name,
            NULLIF(TRIM(A1_CGC), '') as cpf
        FROM silver.clientes 
        WHERE is_deleted = FALSE
        
        UNION
        
        SELECT 
            instance_id,
            NULLIF(TRIM(F2_PACIENT), '') as entity_id,
            NULLIF(TRIM(F2_NOMPACI), '') as entity_name,
            NULLIF(TRIM(F2_CPFPACI), '') as cpf
        FROM silver.notas 
        WHERE is_deleted = FALSE AND F2_PACIENT IS NOT NULL
        
        UNION
        
        SELECT 
            instance_id,
            NULLIF(TRIM(COALESCE(C5_ZCODPAC, C6_CLI, C5_CLIENTE)), '') as entity_id,
            NULLIF(TRIM(COALESCE(C5_ZPACIEN, C5_NOMCLI)), '') as entity_name,
            NULL as cpf
        FROM silver.pedidos 
        WHERE is_deleted = FALSE
        
        UNION
        
        SELECT 
            instance_id,
            NULLIF(TRIM(COALESCE(L1_PACIENT, L1_CLIRESP, L1_CLIENTE)), '') as entity_id,
            NULLIF(TRIM(L1_NOMPACI), '') as entity_name,
            NULLIF(TRIM(L1_CPFPACI), '') as cpf
        FROM silver.venda_direta 
        WHERE is_deleted = FALSE
    )
    SELECT DISTINCT
        instance_id,
        entity_id,
        entity_name,
        cpf,
        CAST(-1 AS BIGINT) as prontuario
    FROM all_entities
    WHERE entity_id IS NOT NULL AND entity_name IS NOT NULL;
    """)
    
    cnt = con.execute("SELECT count(*) FROM gold._stage_dim_paciente_crosswalk").fetchone()[0]
    logger.info(f"Extracted {cnt:,} distinct entities for crosswalk matching.")
    
    find_prontuarios(
        source_con=con,
        clinisys_db_path=CLINISYS_DB_PATH,
        source_schema='gold',
        source_table='_stage_dim_paciente_crosswalk',
        id_col='entity_id',
        name_col='entity_name',
        birthdate_col=None,
        cpf_col='cpf',
        label='paciente_crosswalk',
        suffix='',
    )
    
    con.execute("""
    CREATE OR REPLACE TABLE gold.dim_paciente_crosswalk AS
    WITH ranked AS (
        SELECT 
            instance_id,
            entity_id,
            entity_name,
            cpf,
            prontuario,
            ROW_NUMBER() OVER(
                PARTITION BY instance_id, entity_id 
                ORDER BY 
                    CASE WHEN prontuario > 0 THEN 0 ELSE 1 END,
                    CASE WHEN cpf IS NOT NULL THEN 0 ELSE 1 END,
                    LENGTH(entity_name) DESC
            ) as rn
        FROM gold._stage_dim_paciente_crosswalk
    )
    SELECT 
        instance_id,
        entity_id,
        entity_name,
        cpf,
        prontuario
    FROM ranked
    WHERE rn = 1;
    """)
    
    con.execute("DROP TABLE IF EXISTS gold._stage_dim_paciente_crosswalk")
    
    stats = con.execute("""
        SELECT 
            instance_id,
            count(*) as total,
            count(CASE WHEN prontuario > 0 THEN 1 END) as matched,
            round(count(CASE WHEN prontuario > 0 THEN 1 END) * 100.0 / count(*), 2) as rate
        FROM gold.dim_paciente_crosswalk
        GROUP BY instance_id
    """).fetchall()
    logger.info("Crosswalk dimension built successfully:")
    for s in stats:
        logger.info(f"  Instance {s[0]}: Total={s[1]:,} | Matched={s[2]:,} ({s[3]}%)")


def create_gold_table(con):
    logger.info("Calculating filling rates for the new columns in silver.produtos...")
    try:
        stats = con.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN B1_ZDGEREN IS NOT NULL AND TRIM(B1_ZDGEREN) != '' THEN 1 END) as filled_geren,
                COUNT(CASE WHEN B1_ZMAPING IS NOT NULL AND TRIM(B1_ZMAPING) != '' THEN 1 END) as filled_maping,
                COUNT(CASE WHEN B1_ZCICLOS IS NOT NULL AND TRIM(B1_ZCICLOS) != '' THEN 1 END) as filled_ciclos
            FROM silver.produtos
        """).fetchone()
        
        total = stats[0] if stats[0] else 1
        pct_geren = (stats[1] / total) * 100
        pct_maping = (stats[2] / total) * 100
        pct_ciclos = (stats[3] / total) * 100
        
        logger.info(f"New product columns filling rates (silver.produtos):")
        logger.info(f"  B1_ZDGEREN (Descrição Gerencial): {stats[1]}/{total} ({pct_geren:.2f}%)")
        logger.info(f"  B1_ZMAPING (Descrição Mapping): {stats[2]}/{total} ({pct_maping:.2f}%)")
        logger.info(f"  B1_ZCICLOS (Ciclos): {stats[3]}/{total} ({pct_ciclos:.2f}%)")
    except Exception as e:
        logger.warning(f"Could not calculate product column filling rates: {e}")

    logger.info("Combining Silver tables into gold.protheus_notas_faturadas...")

    con.execute("DROP TABLE IF EXISTS gold.protheus_notas_faturadas")

    query = """
    CREATE TABLE gold.protheus_notas_faturadas AS
    SELECT
        COALESCE(NULLIF(TRIM(c_cli.A1_CODMS), ''), NULLIF(TRIM(c_cli.A1_COD), ''), NULLIF(TRIM(n.F2_CLIENTE), '')) AS cliente_id,
        COALESCE(c_cli.A1_NOME, n.F2_NOME, n.F2_NOMPACI, n.F2_PACIENT) AS nome_cliente,
        COALESCE(
            NULLIF(TRIM(c_pac.A1_CODMS), ''),
            NULLIF(TRIM(c_cli.A1_CODMS), ''),
            NULLIF(TRIM(c_pac.A1_COD), ''),
            NULLIF(TRIM(n.F2_PACIENT), ''),
            NULLIF(TRIM(n.F2_CLIENTE), '')
        ) AS paciente_id,
        COALESCE(
            n.F2_NOMPACI,
            CASE WHEN n.instance_id = 'BH' THEN n.F2_PACIENT ELSE NULL END,
            c_pac.A1_NOME,
            n.F2_NOME
        ) AS nome_paciente,
        COALESCE(n.F2_CPFPACI, c_cli.A1_CGC) AS cpf,
        COALESCE(
            NULLIF(cw_pac.prontuario, -1),
            NULLIF(cw_cli.prontuario, -1),
            -1
        ) AS prontuario,
        CAST(n.F2_EMISSAO AS TIMESTAMP) AS dt_emissao,
        p.B1_DESC AS descricao_produto,
        n.D2_QUANT AS quantidade,
        n.D2_TOTAL AS valor_total,
        p.B1_ZDGEREN AS descricao_gerencial,
        NULLIF(TRIM(n.F2_FILIAL), '') AS filial,
        n.D2_TES AS tipo_nota,
        NULLIF(TRIM(n.F2_DOC), '') AS num_nota,
        n.F2_SERIE AS serie_nota,
        n.F2_NFELETR AS nf_eletr,
        NULLIF(TRIM(n.F2_VEND1), '') AS medico_id,
        v.A3_NOME AS nome_medico,
        n.F2_CLIENTE AS cliente_totvs,
        n.F2_USERLGI AS operador,
        NULLIF(TRIM(n.D2_COD), '') AS produto_id,
        COALESCE(TRY_CAST(n.D2_TOTAL AS DOUBLE), 0.0) AS valor_mercadoria,
        COALESCE(TRY_CAST(n.D2_CUSTO1 AS DOUBLE), 0.0) AS valor_custo,
        COALESCE(TRY_CAST(n.D2_CUSTO2 AS DOUBLE), 0.0) AS valor_custo_unit,
        COALESCE(TRY_CAST(n.D2_DESC AS DOUBLE), 0.0) AS valor_desconto,
        CASE
            -- BH Instance (ProCriar)
            WHEN n.instance_id = 'BH' AND n.F2_FILIAL IN ('0102', '0201') THEN 'Pouso Alegre'
            WHEN n.instance_id = 'BH' THEN 'Belo Horizonte'
            -- Company 01 (Ibirapuera / Vila Mariana)
            WHEN n.company_id = '01' AND n.F2_FILIAL IN ('010101', '010150') THEN 'Ibirapuera'
            WHEN n.company_id = '01' AND n.F2_FILIAL IN ('010155', '010104', '010106') THEN 'Vila Mariana'
            -- Company 03 (Campinas)
            WHEN n.company_id = '03' AND n.F2_FILIAL = '030101' THEN 'Campinas'
            -- Company 06 (Pro Fiv / Santa Joana)
            WHEN n.company_id = '06' AND n.F2_FILIAL = '060101' THEN 'Pro Fiv'
            -- Company 05 (Belo Horizonte - legacy / Huntington)
            WHEN n.company_id = '05' AND n.F2_FILIAL = '0101' THEN 'Belo Horizonte'
            -- Company 07 (Salvador - Cenafert / FIV Brasilia)
            WHEN n.company_id = '07' AND n.F2_FILIAL IN ('010101', '020101') THEN 'Salvador - Cenafert'
            WHEN n.company_id = '07' AND n.F2_FILIAL IN ('030101') THEN 'FIV Brasilia'
            WHEN n.company_id = '07' AND n.F2_FILIAL IN ('040101', '040102') THEN 'Rio de Janeiro'
            ELSE 'Unknown Unit (' || COALESCE(n.company_id, '') || ', ' || COALESCE(n.F2_FILIAL, '') || ')'
        END AS unidade,
        MONTH(n.F2_EMISSAO) AS mes,
        YEAR(n.F2_EMISSAO) AS ano,
        n.D2_CONTA AS conta_contabil,
        CAST(NULL AS VARCHAR) AS interno_externo,
        p.B1_ZMAPING AS descricao_mapping_actividad,
        TRY_CAST(p.B1_ZCICLOS AS INTEGER) AS ciclos,
        0 AS qnt_cons,
        CASE 
            WHEN n.instance_id = 'BH' THEN '5'
            WHEN n.company_id = '01' THEN '1'
            WHEN n.company_id = '03' THEN '3'
            WHEN n.company_id = '05' THEN '5'
            WHEN n.company_id = '06' THEN '6'
            WHEN n.company_id = '07' THEN '7'
            ELSE 'Unknown'
        END AS grp,
        t.F4_TEXTO AS descricao_tes,
        'False' AS fez_ciclo,
        ROW_NUMBER() OVER (ORDER BY n.F2_EMISSAO DESC, n.F2_DOC DESC, n.D2_ITEM ASC) AS line_number,
        n.extraction_timestamp AS extraction_timestamp,
        'Protheus API' AS file_name,
        n.instance_id AS instance_id
    FROM silver.notas n
    LEFT JOIN silver.clientes c_cli
        ON n.instance_id = c_cli.instance_id AND n.F2_CLIENTE = c_cli.A1_COD AND n.F2_LOJA = c_cli.A1_LOJA
    LEFT JOIN silver.clientes c_pac
        ON n.instance_id = c_pac.instance_id AND n.F2_PACIENT = c_pac.A1_COD AND COALESCE(c_pac.A1_LOJA, '01') = '01'
    LEFT JOIN silver.produtos p
        ON n.instance_id = p.instance_id AND n.D2_COD = p.B1_COD
    LEFT JOIN silver.vendedores v
        ON n.instance_id = v.instance_id AND n.F2_VEND1 = v.A3_COD
    LEFT JOIN silver.tes t
        ON n.instance_id = t.instance_id AND n.D2_TES = t.F4_CODIGO
    LEFT JOIN gold.dim_paciente_crosswalk cw_pac
        ON n.instance_id = cw_pac.instance_id 
       AND COALESCE(NULLIF(TRIM(c_pac.A1_CODMS), ''), NULLIF(TRIM(c_cli.A1_CODMS), ''), NULLIF(TRIM(c_pac.A1_COD), ''), NULLIF(TRIM(n.F2_PACIENT), ''), NULLIF(TRIM(n.F2_CLIENTE), '')) = cw_pac.entity_id
    LEFT JOIN gold.dim_paciente_crosswalk cw_cli
        ON n.instance_id = cw_cli.instance_id 
       AND COALESCE(NULLIF(TRIM(c_cli.A1_CODMS), ''), NULLIF(TRIM(c_cli.A1_COD), ''), NULLIF(TRIM(n.F2_CLIENTE), '')) = cw_cli.entity_id
    WHERE n.is_deleted = FALSE;
    """

    con.execute(query)
    count = con.execute("SELECT COUNT(*) FROM gold.protheus_notas_faturadas").fetchone()[0]
    logger.info(f"Created gold.protheus_notas_faturadas with {count:,} rows")

    # Log final statistics
    stats = con.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN prontuario IS NOT NULL AND prontuario != -1 THEN 1 END) as matched,
            COUNT(CASE WHEN prontuario IS NULL OR prontuario = -1 THEN 1 END) as unmatched
        FROM gold.protheus_notas_faturadas
    """).fetchone()
    rate = stats[1] / stats[0] * 100 if stats[0] else 0.0
    logger.info(f"Final prontuario matching stats (gold.protheus_notas_faturadas): Total={stats[0]:,}, Matched={stats[1]:,}, Unmatched={stats[2]:,}, Rate={rate:.2f}%")


def update_prontuario_column(con):
    """Deprecated: Matching is now performed directly at table creation via gold.dim_paciente_crosswalk."""
    pass


def create_gold_pedidos_a_faturar_table(con):
    logger.info("Combining Silver tables into gold.protheus_pedidos_a_faturar...")

    con.execute("DROP TABLE IF EXISTS gold.pedidos_de_vendas")
    con.execute("DROP TABLE IF EXISTS gold.protheus_pedidos_de_vendas")
    con.execute("DROP TABLE IF EXISTS gold.protheus_pedidos_a_faturar")

    query = """
    CREATE TABLE gold.protheus_pedidos_a_faturar AS
    WITH notas_dedup AS (
        SELECT 
            instance_id,
            company_id,
            F2_FILIAL,
            D2_PEDIDO,
            D2_ITEMPV,
            D2_COD,
            NULLIF(TRIM(F2_DOC), '') AS F2_DOC,
            F2_SERIE,
            F2_NFELETR,
            CAST(F2_EMISSAO AS TIMESTAMP) AS dt_nota,
            ROW_NUMBER() OVER(
                PARTITION BY instance_id, company_id, F2_FILIAL, D2_PEDIDO, D2_ITEMPV, D2_COD 
                ORDER BY F2_EMISSAO DESC, F2_DOC DESC
            ) as rn
        FROM silver.notas
        WHERE is_deleted = FALSE
          AND D2_PEDIDO IS NOT NULL 
          AND TRIM(D2_PEDIDO) != ''
    ),
    l1_dedup AS (
        -- Patient resolution via SL10X0 L1 (silver.venda_direta)
        SELECT 
            instance_id,
            company_id,
            L1_FILIAL,
            L1_PEDRES,
            COALESCE(L1_PACIENT, L1_CLIRESP, L1_CLIENTE) AS L1_PACIENT,
            COALESCE(L1_NOMPACI, '') AS L1_NOMPACI,
            L1_CPFPACI,
            ROW_NUMBER() OVER(
                PARTITION BY instance_id, company_id, L1_FILIAL, L1_PEDRES 
                ORDER BY extraction_timestamp DESC
            ) as rn
        FROM silver.venda_direta
        WHERE is_deleted = FALSE 
          AND L1_PEDRES IS NOT NULL 
          AND TRIM(L1_PEDRES) != ''
    )
    SELECT
        CASE 
            WHEN p.instance_id = 'BH' THEN '5'
            ELSE p.company_id 
        END AS grp,
        CASE
            -- BH Instance (ProCriar)
            WHEN p.instance_id = 'BH' AND p.C5_FILIAL IN ('0102', '0201') THEN 'Pouso Alegre'
            WHEN p.instance_id = 'BH' THEN 'Belo Horizonte'
            -- Company 01 (Ibirapuera / Vila Mariana)
            WHEN p.company_id = '01' AND p.C5_FILIAL IN ('010101', '010150') THEN 'Ibirapuera'
            WHEN p.company_id = '01' AND p.C5_FILIAL IN ('010155', '010104', '010106') THEN 'Vila Mariana'
            -- Company 03 (Campinas)
            WHEN p.company_id = '03' AND p.C5_FILIAL = '030101' THEN 'Campinas'
            -- Company 06 (Pro Fiv / Santa Joana)
            WHEN p.company_id = '06' AND p.C5_FILIAL = '060101' THEN 'Pro Fiv'
            -- Company 05 (Belo Horizonte - legacy / Huntington)
            WHEN p.company_id = '05' AND p.C5_FILIAL = '0101' THEN 'Belo Horizonte'
            -- Company 07 (Salvador - Cenafert / FIV Brasilia)
            WHEN p.company_id = '07' AND p.C5_FILIAL IN ('010101', '020101') THEN 'Salvador - Cenafert'
            WHEN p.company_id = '07' AND p.C5_FILIAL IN ('030101') THEN 'FIV Brasilia'
            WHEN p.company_id = '07' AND p.C5_FILIAL IN ('040101', '040102') THEN 'Rio de Janeiro'
            ELSE 'Unknown Unit (' || COALESCE(p.company_id, '') || ', ' || COALESCE(p.C5_FILIAL, '') || ')'
        END AS unidade,
        NULLIF(TRIM(p.C5_FILIAL), '') AS filial,
        NULLIF(TRIM(p.C5_NUM), '') AS pedido,
        NULLIF(TRIM(p.C5_ORCRES), '') AS orcamento,
        COALESCE(NULLIF(TRIM(c_cli_cli.A1_CODMS), ''), NULLIF(TRIM(c_cli_cli.A1_COD), ''), NULLIF(TRIM(p.C5_CLIENTE), '')) AS cliente_id,
        c_cli_cli.A1_NOME AS nome_cliente,
        COALESCE(l1.L1_CPFPACI, c_l1_pac.A1_CGC, c_cli_pat.A1_CGC, c_cli_cli.A1_CGC) AS cpf,
        COALESCE(
            NULLIF(cw_pac.prontuario, -1),
            NULLIF(cw_cli.prontuario, -1),
            -1
        ) AS prontuario,
        -- Patient resolution: A1_CODMS priority (Huntington), fallback to raw IDs (BH)
        COALESCE(
            NULLIF(TRIM(c_cli_pat.A1_CODMS), ''),
            NULLIF(TRIM(c_l1_pac.A1_CODMS), ''),
            NULLIF(TRIM(c_zcodpac.A1_CODMS), ''),
            NULLIF(TRIM(c_cli_cli.A1_CODMS), ''),
            NULLIF(TRIM(l1.L1_PACIENT), ''),
            NULLIF(TRIM(p.C5_ZCODPAC), ''),
            NULLIF(TRIM(p.C6_CLI), ''),
            NULLIF(TRIM(p.C5_CLIENTE), '')
        ) AS paciente_id,
        COALESCE(NULLIF(l1.L1_NOMPACI, ''), p.C5_ZPACIEN, c_l1_pac.A1_NOME, c_cli_pat.A1_NOME, c_cli_cli.A1_NOME) AS nome_paciente,
        NULLIF(TRIM(p.C5_VEND1), '') AS medico_id,
        v.A3_NOME AS nome_medico,
        CAST(p.C5_EMISSAO AS TIMESTAMP) AS dt_emissao,
        COALESCE(NULLIF(TRIM(n_item.F2_DOC), ''), NULLIF(TRIM(p.C6_NOTA), '')) AS num_nota,
        COALESCE(n_item.F2_SERIE, p.C6_SERIE) AS serie_nota,
        TRY_CAST(prod.B1_GRUPO AS INTEGER) AS grupo_produto,
        NULLIF(TRIM(p.C6_PRODUTO), '') AS produto_id,
        COALESCE(prod.B1_DESC, p.C6_DESCRI) AS descricao_produto,
        p.C6_QTDVEN AS quantidade,
        p.C6_PRCVEN AS valor_unitario,
        p.C6_VALOR AS valor_mercadoria,
        0.0 AS valor_iss,
        TRY_CAST(p.C6_VALDESC AS DOUBLE) AS valor_desconto,
        0.0 AS valor_custo,
        TRY_CAST(p.C6_PRUNIT AS DOUBLE) AS valor_custo_unit,
        0.0 AS valor_comissao,
        p.C6_VALOR AS valor_total,
        COALESCE(TRY_CAST(p.C6_VALOR AS DOUBLE), 0.0) + COALESCE(TRY_CAST(p.C6_VALDESC AS DOUBLE), 0.0) AS valor_bruto,
        NULLIF(TRIM(p.C5_CONDPAG), '') AS condicao_pagamento,
        p.C5_USERLGI AS operador,
        TRY_CAST(p.C6_PRUNIT AS DOUBLE) AS preco_venda,
        0.0 AS ultimo_preco,
        p.instance_id AS instance_id
    FROM silver.pedidos p
    LEFT JOIN silver.clientes c_cli_cli
        ON p.instance_id = c_cli_cli.instance_id AND p.C5_CLIENTE = c_cli_cli.A1_COD AND p.C5_LOJACLI = c_cli_cli.A1_LOJA
    LEFT JOIN l1_dedup l1
        ON p.instance_id = l1.instance_id AND p.company_id = l1.company_id AND p.C5_FILIAL = l1.L1_FILIAL AND p.C5_NUM = l1.L1_PEDRES AND l1.rn = 1
    LEFT JOIN silver.clientes c_l1_pac
        ON p.instance_id = c_l1_pac.instance_id AND l1.L1_PACIENT = c_l1_pac.A1_COD AND COALESCE(c_l1_pac.A1_LOJA, '01') = '01'
    LEFT JOIN silver.clientes c_cli_pat
        ON p.instance_id = c_cli_pat.instance_id AND p.C6_CLI = c_cli_pat.A1_COD AND p.C5_LOJACLI = c_cli_pat.A1_LOJA
    LEFT JOIN silver.clientes c_zcodpac
        ON p.instance_id = c_zcodpac.instance_id AND p.C5_ZCODPAC = c_zcodpac.A1_COD AND COALESCE(c_zcodpac.A1_LOJA, '01') = '01'
    LEFT JOIN silver.produtos prod
        ON p.instance_id = prod.instance_id AND p.C6_PRODUTO = prod.B1_COD
    LEFT JOIN silver.vendedores v
        ON p.instance_id = v.instance_id AND p.C5_VEND1 = v.A3_COD
    LEFT JOIN notas_dedup n_item
        ON p.instance_id = n_item.instance_id
       AND p.company_id = n_item.company_id
       AND p.C5_FILIAL = n_item.F2_FILIAL
       AND p.C5_NUM = n_item.D2_PEDIDO 
       AND p.C6_ITEM = n_item.D2_ITEMPV
       AND p.C6_PRODUTO = n_item.D2_COD 
       AND n_item.rn = 1
    LEFT JOIN gold.dim_paciente_crosswalk cw_pac
        ON p.instance_id = cw_pac.instance_id 
       AND COALESCE(
            NULLIF(TRIM(c_cli_pat.A1_CODMS), ''),
            NULLIF(TRIM(c_l1_pac.A1_CODMS), ''),
            NULLIF(TRIM(c_zcodpac.A1_CODMS), ''),
            NULLIF(TRIM(c_cli_cli.A1_CODMS), ''),
            NULLIF(TRIM(l1.L1_PACIENT), ''),
            NULLIF(TRIM(p.C5_ZCODPAC), ''),
            NULLIF(TRIM(p.C6_CLI), ''),
            NULLIF(TRIM(p.C5_CLIENTE), '')
        ) = cw_pac.entity_id
    LEFT JOIN gold.dim_paciente_crosswalk cw_cli
        ON p.instance_id = cw_cli.instance_id 
       AND COALESCE(NULLIF(TRIM(c_cli_cli.A1_CODMS), ''), NULLIF(TRIM(c_cli_cli.A1_COD), ''), NULLIF(TRIM(p.C5_CLIENTE), '')) = cw_cli.entity_id
    WHERE p.is_deleted = FALSE
      -- Point 2: Budget Identifier Filtering (C5_ORCRES <> '')
      AND p.C5_ORCRES IS NOT NULL AND TRIM(p.C5_ORCRES) != ''
      -- Point 3: Residue / Cancellation Filtering (C6_BLQ != 'R')
      AND (p.C6_BLQ IS NULL OR p.C6_BLQ != 'R');
    """

    con.execute(query)
    count = con.execute("SELECT COUNT(*) FROM gold.protheus_pedidos_a_faturar").fetchone()[0]
    logger.info(f"Created gold.protheus_pedidos_a_faturar with {count:,} rows")

    # Log patient coverage stats
    patient_stats = con.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(CASE WHEN paciente_id != cliente_id THEN 1 END) as patient_differs_from_client,
            COUNT(CASE WHEN nome_paciente IS NOT NULL AND nome_paciente != '' THEN 1 END) as has_patient_name
        FROM gold.protheus_pedidos_a_faturar
    """).fetchone()
    pct_differs = (patient_stats[1] / patient_stats[0] * 100) if patient_stats[0] else 0.0
    pct_has_name = (patient_stats[2] / patient_stats[0] * 100) if patient_stats[0] else 0.0
    logger.info(f"  Patient coverage: {patient_stats[1]:,}/{patient_stats[0]:,} rows where Patient != Client ({pct_differs:.1f}%)")
    logger.info(f"  Patient name coverage: {patient_stats[2]:,}/{patient_stats[0]:,} rows with patient name ({pct_has_name:.1f}%)")

    # Log final statistics for pedidos_a_faturar
    stats = con.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN prontuario IS NOT NULL AND prontuario != -1 THEN 1 END) as matched,
            COUNT(CASE WHEN prontuario IS NULL OR prontuario = -1 THEN 1 END) as unmatched
        FROM gold.protheus_pedidos_a_faturar
    """).fetchone()
    rate = stats[1] / stats[0] * 100 if stats[0] else 0.0
    logger.info(f"Final prontuario matching stats (gold.protheus_pedidos_a_faturar): Total={stats[0]:,}, Matched={stats[1]:,}, Unmatched={stats[2]:,}, Rate={rate:.2f}%")


def update_prontuario_column_pedidos(con):
    """Deprecated: Matching is now performed directly at table creation via gold.dim_paciente_crosswalk."""
    pass


def create_gold_vendas_consolidadas_table(con):
    logger.info("Combining silver.venda_direta and gold.protheus_pedidos_a_faturar into gold.protheus_vendas_consolidadas (with deduplication)...")

    con.execute("DROP TABLE IF EXISTS gold.protheus_vendas_consolidadas")

    query = """
    CREATE TABLE gold.protheus_vendas_consolidadas AS
    WITH common_window AS (
        SELECT 
            GREATEST(
                (SELECT MIN(L1_EMISSAO) FROM silver.venda_direta WHERE is_deleted = FALSE),
                (SELECT MIN(CAST(dt_emissao AS DATE)) FROM gold.protheus_pedidos_a_faturar)
            ) AS start_date,
            LEAST(
                (SELECT MAX(L1_EMISSAO) FROM silver.venda_direta WHERE is_deleted = FALSE),
                (SELECT MAX(CAST(dt_emissao AS DATE)) FROM gold.protheus_pedidos_a_faturar)
            ) AS end_date
    ),
    invoiced_pedidos AS (
        SELECT DISTINCT 
            instance_id, company_id, C5_FILIAL, C5_NUM 
        FROM silver.pedidos 
        WHERE is_deleted = FALSE 
          AND (
              (C5_NOTA IS NOT NULL AND TRIM(C5_NOTA) != '') 
              OR (C6_NOTA IS NOT NULL AND TRIM(C6_NOTA) != '')
          )
    ),
    ped_dates AS (
        SELECT 
            instance_id,
            company_id, 
            C5_FILIAL, 
            C5_NUM, 
            MIN(CAST(C5_EMISSAO AS TIMESTAMP)) AS dt_pedido
        FROM silver.pedidos 
        WHERE is_deleted = FALSE
        GROUP BY 1, 2, 3, 4
    ),
    orc_dates AS (
        SELECT 
            instance_id,
            company_id, 
            L1_FILIAL, 
            COALESCE(NULLIF(TRIM(L1_NUM), ''), NULLIF(TRIM(L1_ORCRES), '')) AS orc_num, 
            MIN(CAST(L1_EMISSAO AS TIMESTAMP)) AS dt_orcamento
        FROM silver.venda_direta 
        WHERE is_deleted = FALSE
        GROUP BY 1, 2, 3, 4
    ),
    ped_item_nota AS (
        SELECT 
            n.instance_id,
            n.company_id,
            n.F2_FILIAL,
            n.D2_PEDIDO,
            n.D2_ITEMPV,
            n.D2_COD,
            NULLIF(TRIM(n.F2_DOC), '') AS num_nota,
            n.F2_SERIE AS serie_nota,
            CAST(n.F2_EMISSAO AS TIMESTAMP) AS dt_nota,
            ROW_NUMBER() OVER(
                PARTITION BY n.instance_id, n.company_id, n.F2_FILIAL, n.D2_PEDIDO, n.D2_ITEMPV, n.D2_COD
                ORDER BY n.F2_EMISSAO DESC, n.F2_DOC DESC
            ) AS rn
        FROM silver.notas n
        WHERE n.is_deleted = FALSE
          AND n.D2_PEDIDO IS NOT NULL AND TRIM(n.D2_PEDIDO) != ''
    ),
    vd_item_nota AS (
        SELECT 
            instance_id,
            company_id,
            F2_FILIAL,
            NULLIF(TRIM(F2_DOC), '') AS F2_DOC,
            F2_SERIE,
            MIN(CAST(F2_EMISSAO AS TIMESTAMP)) AS dt_nota
        FROM silver.notas 
        WHERE is_deleted = FALSE
        GROUP BY 1, 2, 3, 4, 5
    ),
    vd_ped_exists AS (
        SELECT DISTINCT 
            instance_id,
            L1_FILIAL, 
            NULLIF(TRIM(L1_PEDRES), '') as pedido
        FROM silver.venda_direta 
        WHERE is_deleted = FALSE AND (L1_SITUA IS NULL OR L1_SITUA NOT IN ('FR', 'CA'))
          AND L1_PEDRES IS NOT NULL AND TRIM(L1_PEDRES) != ''
    ),
    vd_orc_exists AS (
        SELECT DISTINCT 
            instance_id,
            L1_FILIAL, 
            COALESCE(NULLIF(TRIM(L1_NUM), ''), NULLIF(TRIM(L1_ORCRES), '')) as orcamento
        FROM silver.venda_direta 
        WHERE is_deleted = FALSE AND (L1_SITUA IS NULL OR L1_SITUA NOT IN ('FR', 'CA'))
          AND COALESCE(NULLIF(TRIM(L1_NUM), ''), NULLIF(TRIM(L1_ORCRES), '')) IS NOT NULL
    ),
    venda_direta_rows AS (
        SELECT 
            COALESCE(
                NULLIF(cw_pac.prontuario, -1),
                NULLIF(cw_cli.prontuario, -1),
                -1
            ) AS prontuario,
            CASE 
                WHEN v.instance_id = 'BH' THEN '5'
                ELSE v.company_id 
            END AS grp,
            CASE
                WHEN v.instance_id = 'BH' AND v.L1_FILIAL IN ('0102', '0201') THEN 'Pouso Alegre'
                WHEN v.instance_id = 'BH' THEN 'Belo Horizonte'
                WHEN v.company_id = '01' AND v.L1_FILIAL IN ('010101', '010150') THEN 'Ibirapuera'
                WHEN v.company_id = '01' AND v.L1_FILIAL IN ('010155', '010104', '010106') THEN 'Vila Mariana'
                WHEN v.company_id = '03' AND v.L1_FILIAL = '030101' THEN 'Campinas'
                WHEN v.company_id = '06' AND v.L1_FILIAL = '060101' THEN 'Pro Fiv'
                WHEN v.company_id = '05' AND v.L1_FILIAL = '0101' THEN 'Belo Horizonte'
                WHEN v.company_id = '07' AND v.L1_FILIAL IN ('010101', '020101') THEN 'Salvador - Cenafert'
                WHEN v.company_id = '07' AND v.L1_FILIAL IN ('030101') THEN 'FIV Brasilia'
                WHEN v.company_id = '07' AND v.L1_FILIAL IN ('040101', '040102') THEN 'Rio de Janeiro'
                ELSE 'Unknown Unit (' || COALESCE(v.company_id, '') || ', ' || COALESCE(v.L1_FILIAL, '') || ')'
            END AS unidade,
            NULLIF(TRIM(v.L1_FILIAL), '') AS filial,
            COALESCE(NULLIF(TRIM(v.L1_NUM), ''), NULLIF(TRIM(v.L1_ORCRES), '')) AS orcamento,
            CAST(v.L1_EMISSAO AS TIMESTAMP) AS dt_orcamento,
            COALESCE(NULLIF(TRIM(v.L1_PEDRES), ''), NULLIF(TRIM(v.L2_PEDRES), '')) AS pedido,
            p_dt.dt_pedido AS dt_pedido,
            CASE 
                WHEN v.L1_PEDRES IS NOT NULL AND TRIM(v.L1_PEDRES) != '' THEN pn.num_nota
                ELSE COALESCE(NULLIF(TRIM(v.L2_DOC), ''), NULLIF(TRIM(vn.F2_DOC), ''), NULLIF(TRIM(v.L1_DOC), ''))
            END AS num_nota,
            CASE 
                WHEN v.L1_PEDRES IS NOT NULL AND TRIM(v.L1_PEDRES) != '' THEN pn.serie_nota
                ELSE COALESCE(v.L2_SERIE, vn.F2_SERIE, v.L1_SERIE)
            END AS serie_nota,
            CASE 
                WHEN v.L1_PEDRES IS NOT NULL AND TRIM(v.L1_PEDRES) != '' THEN pn.dt_nota
                ELSE vn.dt_nota
            END AS dt_nota,
            COALESCE(prod.B1_DESC, v.L2_DESCRI) AS descricao_produto,
            TRY_CAST(v.L2_VLRITEM AS DOUBLE) AS valor_total,
            CAST(v.L1_EMISSAO AS TIMESTAMP) AS dt_emissao,
            CASE 
                WHEN (v.L1_PEDRES IS NOT NULL AND TRIM(v.L1_PEDRES) != '') AND pn.num_nota IS NOT NULL 
                THEN 'FATURADO_VIA_PEDIDO'
                
                WHEN (v.L1_PEDRES IS NOT NULL AND TRIM(v.L1_PEDRES) != '') 
                THEN 'PEDIDO_A_FATURAR'
                
                WHEN COALESCE(NULLIF(TRIM(v.L2_DOC), ''), NULLIF(TRIM(vn.F2_DOC), ''), NULLIF(TRIM(v.L1_DOC), '')) IS NOT NULL 
                THEN 'FATURADO_DIRETO'
                
                WHEN v.L1_SITUA = 'FR' 
                THEN 'ORCAMENTO_FECHADO'
                
                ELSE 'ORCAMENTO_ABERTO'
            END AS status_fluxo,
            COALESCE(NULLIF(TRIM(c_cli.A1_CODMS), ''), NULLIF(TRIM(c_cli.A1_COD), ''), NULLIF(TRIM(v.L1_CLIENTE), '')) AS cliente_id,
            c_cli.A1_NOME AS nome_cliente,
            COALESCE(
                NULLIF(TRIM(c_pac.A1_CODMS), ''),
                NULLIF(TRIM(c_cli.A1_CODMS), ''),
                NULLIF(TRIM(v.L1_PACIENT), ''),
                NULLIF(TRIM(v.L1_CLIRESP), ''),
                NULLIF(TRIM(c_pac.A1_COD), ''),
                NULLIF(TRIM(v.L1_CLIENTE), '')
            ) AS paciente_id,
            COALESCE(v.L1_NOMPACI, c_pac.A1_NOME, c_cli.A1_NOME) AS nome_paciente,
            NULLIF(TRIM(v.L1_VEND), '') AS medico_id,
            vend.A3_NOME AS nome_medico,
            NULLIF(TRIM(v.L2_PRODUTO), '') AS produto_id,
            TRY_CAST(prod.B1_GRUPO AS INTEGER) AS grupo_produto,
            prod.B1_ZDGEREN AS descricao_gerencial,
            prod.B1_ZMAPING AS descricao_mapping_actividad,
            TRY_CAST(prod.B1_ZCICLOS AS INTEGER) AS ciclos,
            TRY_CAST(v.L2_QUANT AS DOUBLE) AS quantidade,
            TRY_CAST(v.L2_VRUNIT AS DOUBLE) AS valor_unitario,
            TRY_CAST(v.L2_VLRITEM AS DOUBLE) AS valor_mercadoria,
            COALESCE(
                CASE WHEN TRY_CAST(v.L2_VALDESC AS DOUBLE) > 0 THEN TRY_CAST(v.L2_VALDESC AS DOUBLE) ELSE NULL END,
                CASE WHEN TRY_CAST(v.L2_DESC AS DOUBLE) != 0 
                     THEN (TRY_CAST(v.L2_VRUNIT AS DOUBLE) * TRY_CAST(v.L2_QUANT AS DOUBLE)) * (ABS(TRY_CAST(v.L2_DESC AS DOUBLE)) / 100.0)
                     ELSE 0.0 
                END,
                0.0
            ) AS valor_desconto,
            COALESCE(TRY_CAST(v.L2_CUSTO1 AS DOUBLE), 0.0) AS valor_custo,
            COALESCE(TRY_CAST(v.L2_CUSTO2 AS DOUBLE), 0.0) AS valor_custo_unit,
            COALESCE(TRY_CAST(v.L2_VALISS AS DOUBLE), 0.0) AS valor_iss,
            0.0 AS valor_comissao,
            v.L1_FORMPG AS forma_pagamento,
            v.L1_CONDPG AS condicao_pagamento,
            v.L1_OPERADO AS operador,
            COALESCE(c_cli.A1_CGC, v.L1_CPFPACI) AS cpf,
            YEAR(v.L1_EMISSAO) AS ano,
            MONTH(v.L1_EMISSAO) AS mes,
            'VENDA_DIRETA' AS origem,
            v.extraction_timestamp AS extraction_timestamp,
            v.instance_id AS instance_id
        FROM silver.venda_direta v
        LEFT JOIN invoiced_pedidos inv 
          ON v.instance_id = inv.instance_id
         AND v.company_id = inv.company_id 
         AND v.L1_FILIAL = inv.C5_FILIAL 
         AND v.L1_PEDRES = inv.C5_NUM
        LEFT JOIN ped_dates p_dt
          ON v.instance_id = p_dt.instance_id
         AND v.company_id = p_dt.company_id
         AND v.L1_FILIAL = p_dt.C5_FILIAL
         AND v.L1_PEDRES = p_dt.C5_NUM
        LEFT JOIN ped_item_nota pn
          ON v.instance_id = pn.instance_id
         AND v.company_id = pn.company_id
         AND v.L1_FILIAL = pn.F2_FILIAL
         AND v.L1_PEDRES = pn.D2_PEDIDO
         AND v.L2_ITEM = pn.D2_ITEMPV
         AND v.L2_PRODUTO = pn.D2_COD
         AND pn.rn = 1
        LEFT JOIN vd_item_nota vn
          ON v.instance_id = vn.instance_id
         AND v.company_id = vn.company_id
         AND v.L1_FILIAL = vn.F2_FILIAL
         AND COALESCE(NULLIF(TRIM(v.L1_DOC), ''), NULLIF(TRIM(v.L2_DOC), '')) = vn.F2_DOC
        LEFT JOIN silver.clientes c_cli
            ON v.instance_id = c_cli.instance_id AND v.L1_CLIENTE = c_cli.A1_COD AND v.L1_LOJA = c_cli.A1_LOJA
        LEFT JOIN silver.clientes c_pac
            ON v.instance_id = c_pac.instance_id AND v.L1_PACIENT = c_pac.A1_COD AND COALESCE(c_pac.A1_LOJA, '01') = '01'
        LEFT JOIN silver.produtos prod
            ON v.instance_id = prod.instance_id AND v.L2_PRODUTO = prod.B1_COD
        LEFT JOIN silver.vendedores vend
            ON v.instance_id = vend.instance_id AND v.L1_VEND = vend.A3_COD
        LEFT JOIN gold.dim_paciente_crosswalk cw_pac
            ON v.instance_id = cw_pac.instance_id 
           AND COALESCE(
                NULLIF(TRIM(c_pac.A1_CODMS), ''),
                NULLIF(TRIM(c_cli.A1_CODMS), ''),
                NULLIF(TRIM(v.L1_PACIENT), ''),
                NULLIF(TRIM(v.L1_CLIRESP), ''),
                NULLIF(TRIM(c_pac.A1_COD), ''),
                NULLIF(TRIM(v.L1_CLIENTE), '')
            ) = cw_pac.entity_id
        LEFT JOIN gold.dim_paciente_crosswalk cw_cli
            ON v.instance_id = cw_cli.instance_id 
           AND COALESCE(NULLIF(TRIM(c_cli.A1_CODMS), ''), NULLIF(TRIM(c_cli.A1_COD), ''), NULLIF(TRIM(v.L1_CLIENTE), '')) = cw_cli.entity_id
        WHERE v.is_deleted = FALSE
          AND (v.L1_SITUA IS NULL OR v.L1_SITUA NOT IN ('FR', 'CA'))
          AND v.L1_EMISSAO BETWEEN (SELECT start_date FROM common_window) AND (SELECT end_date FROM common_window)
    ),
    pedidos_direct_rows AS (
        SELECT 
            p.prontuario AS prontuario,
            p.grp AS grp,
            p.unidade AS unidade,
            p.filial AS filial,
            p.orcamento AS orcamento,
            o_dt.dt_orcamento AS dt_orcamento,
            p.pedido AS pedido,
            p.dt_emissao AS dt_pedido,
            p.num_nota AS num_nota,
            p.serie_nota AS serie_nota,
            n_dt.dt_nota AS dt_nota,
            p.descricao_produto AS descricao_produto,
            p.valor_total AS valor_total,
            p.dt_emissao AS dt_emissao,
            CASE 
                WHEN p.num_nota IS NOT NULL THEN 'FATURADO_VIA_PEDIDO'
                ELSE 'PEDIDO_A_FATURAR'
            END AS status_fluxo,
            p.cliente_id AS cliente_id,
            p.nome_cliente AS nome_cliente,
            p.paciente_id AS paciente_id,
            p.nome_paciente AS nome_paciente,
            p.medico_id AS medico_id,
            p.nome_medico AS nome_medico,
            p.produto_id AS produto_id,
            p.grupo_produto AS grupo_produto,
            prod.B1_ZDGEREN AS descricao_gerencial,
            prod.B1_ZMAPING AS descricao_mapping_actividad,
            TRY_CAST(prod.B1_ZCICLOS AS INTEGER) AS ciclos,
            p.quantidade AS quantidade,
            p.valor_unitario AS valor_unitario,
            p.valor_mercadoria AS valor_mercadoria,
            COALESCE(p.valor_desconto, 0.0) AS valor_desconto,
            COALESCE(p.valor_custo, 0.0) AS valor_custo,
            COALESCE(p.valor_custo_unit, 0.0) AS valor_custo_unit,
            COALESCE(p.valor_iss, 0.0) AS valor_iss,
            COALESCE(p.valor_comissao, 0.0) AS valor_comissao,
            CAST(NULL AS VARCHAR) AS forma_pagamento,
            p.condicao_pagamento AS condicao_pagamento,
            p.operador AS operador,
            p.cpf AS cpf,
            YEAR(p.dt_emissao) AS ano,
            MONTH(p.dt_emissao) AS mes,
            'PEDIDO_DIRETO' AS origem,
            CURRENT_TIMESTAMP::VARCHAR AS extraction_timestamp,
            p.instance_id AS instance_id
        FROM gold.protheus_pedidos_a_faturar p
        LEFT JOIN orc_dates o_dt
            ON p.instance_id = o_dt.instance_id
           AND p.filial = o_dt.L1_FILIAL
           AND p.orcamento = o_dt.orc_num
        LEFT JOIN (
            SELECT 
                instance_id,
                company_id, 
                F2_FILIAL, 
                NULLIF(TRIM(F2_DOC), '') AS doc_num, 
                MIN(CAST(F2_EMISSAO AS TIMESTAMP)) AS dt_nota
            FROM silver.notas 
            WHERE is_deleted = FALSE
            GROUP BY 1, 2, 3, 4
        ) n_dt
            ON p.instance_id = n_dt.instance_id
           AND p.filial = n_dt.F2_FILIAL
           AND p.num_nota = n_dt.doc_num
        LEFT JOIN silver.produtos prod
            ON p.instance_id = prod.instance_id AND p.produto_id = prod.B1_COD
        LEFT JOIN vd_ped_exists vd_ped
            ON p.instance_id = vd_ped.instance_id
           AND p.filial = vd_ped.L1_FILIAL 
           AND p.pedido = vd_ped.pedido
        LEFT JOIN vd_orc_exists vd_orc
            ON p.instance_id = vd_orc.instance_id
           AND p.filial = vd_orc.L1_FILIAL 
           AND p.orcamento = vd_orc.orcamento
        WHERE CAST(p.dt_emissao AS DATE) BETWEEN (SELECT start_date FROM common_window) AND (SELECT end_date FROM common_window)
          AND vd_ped.pedido IS NULL
          AND vd_orc.orcamento IS NULL
    ),
    unioned AS (
        SELECT * FROM venda_direta_rows
        UNION ALL
        SELECT * FROM pedidos_direct_rows
    )
    SELECT * FROM unioned
    ORDER BY dt_emissao DESC, filial ASC, orcamento DESC, pedido DESC;
    """

    con.execute(query)
    count = con.execute("SELECT COUNT(*) FROM gold.protheus_vendas_consolidadas").fetchone()[0]
    logger.info(f"Created gold.protheus_vendas_consolidadas with {count:,} rows")

    # Log final statistics for vendas_consolidadas
    stats = con.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN prontuario IS NOT NULL AND prontuario != -1 THEN 1 END) as matched,
            COUNT(CASE WHEN prontuario IS NULL OR prontuario = -1 THEN 1 END) as unmatched
        FROM gold.protheus_vendas_consolidadas
    """).fetchone()
    rate = stats[1] / stats[0] * 100 if stats[0] else 0.0
    logger.info(f"Final prontuario matching stats (gold.protheus_vendas_consolidadas): Total={stats[0]:,}, Matched={stats[1]:,}, Unmatched={stats[2]:,}, Rate={rate:.2f}%")


def update_prontuario_column_vendas_consolidadas(con):
    """Deprecated: Matching is now performed directly at table creation via gold.dim_paciente_crosswalk."""
    pass


def main():
    logger.info("=== PROTHEUS SILVER TO GOLD CONSOLIDATION STARTED ===")
    logger.info(f"Target Database: {DUCKDB_PATH}")

    try:
        with duckdb.connect(DUCKDB_PATH) as con:
            con.execute("CREATE SCHEMA IF NOT EXISTS gold")
            create_dim_paciente_crosswalk(con)
            create_gold_table(con)
            create_gold_pedidos_a_faturar_table(con)
            create_gold_vendas_consolidadas_table(con)
            logger.info("=== PROTHEUS SILVER TO GOLD CONSOLIDATION FINISHED SUCCESSFUL ===")
    except Exception as e:
        logger.error(f"Gold Consolidation Failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()

