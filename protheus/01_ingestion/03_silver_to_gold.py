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
        TRY_CAST(c_cli.A1_CODMS AS INTEGER) AS "Cliente",
        COALESCE(c_cli.A1_NOME, n.F2_NOMPACI) AS "Nome",
        TRY_CAST(c_pac.A1_CODMS AS INTEGER)::VARCHAR AS "Paciente",
        n.F2_NOMPACI AS "Nom Paciente",
        c_cli.A1_CGC as CPF,
        TRY_CAST(-1 AS INTEGER) AS prontuario,
        CAST(n.F2_EMISSAO AS TIMESTAMP) AS "DT Emissao",
        p.B1_DESC AS "Descricao",
        n.D2_QUANT AS "Qntd.",
        n.D2_TOTAL AS "Total",
        p.B1_ZDGEREN AS "Descrição Gerencial",
        n.F2_FILIAL AS "Loja",
        n.D2_TES AS "Tipo da nota",
        TRY_CAST(n.F2_DOC AS INTEGER) AS "Numero",
        n.F2_SERIE AS "Serie Docto.",
        n.F2_NFELETR AS "NF Eletr.",
        n.F2_VEND1 AS "Vend. 1",
        v.A3_NOME AS "Médico",
        n.F2_CLIENTE AS "Cliente_totvs",
        n.F2_USERLGI AS "Operador",
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
        CAST(NULL AS VARCHAR) AS "Interno/Externo",
        p.B1_ZMAPING AS "Descrição Mapping Actividad",
        TRY_CAST(p.B1_ZCICLOS AS INTEGER) AS "Ciclos",
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
    count = con.execute("SELECT COUNT(*) FROM gold.protheus_notas_faturadas").fetchone()[0]
    logger.info(f"Created gold.protheus_notas_faturadas with {count:,} rows")


def update_prontuario_column(con):
    logger.info("Updating prontuario column using Strategy L matching (Paciente & Cliente)...")

    # 1. First run: Paciente
    logger.info("Run 1: Matching via Paciente columns...")
    find_prontuarios(
        source_con=con,
        clinisys_db_path=CLINISYS_DB_PATH,
        source_schema='gold',
        source_table='protheus_notas_faturadas',
        id_col='Paciente',
        name_col='Nom Paciente',
        birthdate_col=None,
        cpf_col=None,
        label='protheus_paciente',
        suffix='',
    )

    # 2. Second run: Cliente (matching remaining unmatched)
    logger.info("Run 2: Matching via Cliente columns...")
    find_prontuarios(
        source_con=con,
        clinisys_db_path=CLINISYS_DB_PATH,
        source_schema='gold',
        source_table='protheus_notas_faturadas',
        id_col='Cliente',
        name_col='Nome',
        birthdate_col=None,
        cpf_col='CPF',
        label='protheus_cliente',
        suffix='',
    )

    # Log final statistics
    stats = con.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN prontuario IS NOT NULL AND prontuario != -1 THEN 1 END) as matched,
            COUNT(CASE WHEN prontuario IS NULL OR prontuario = -1 THEN 1 END) as unmatched
        FROM gold.protheus_notas_faturadas
    """).fetchone()
    rate = stats[1] / stats[0] * 100 if stats[0] else 0.0
    logger.info(f"Final prontuario matching stats: Total={stats[0]:,}, Matched={stats[1]:,}, Unmatched={stats[2]:,}, Rate={rate:.2f}%")


def create_gold_pedidos_a_faturar_table(con):
    logger.info("Combining Silver tables into gold.protheus_pedidos_a_faturar...")

    con.execute("DROP TABLE IF EXISTS gold.pedidos_de_vendas")
    con.execute("DROP TABLE IF EXISTS gold.protheus_pedidos_de_vendas")
    con.execute("DROP TABLE IF EXISTS gold.protheus_pedidos_a_faturar")

    query = """
    CREATE TABLE gold.protheus_pedidos_a_faturar AS
    WITH notas_dedup AS (
        SELECT 
            D2_PEDIDO,
            F2_FILIAL,
            D2_COD,
            F2_DOC,
            F2_SERIE,
            F2_NFELETR,
            ROW_NUMBER() OVER(
                PARTITION BY F2_FILIAL, D2_PEDIDO, D2_COD 
                ORDER BY F2_EMISSAO DESC, F2_DOC DESC
            ) as rn
        FROM silver.notas
        WHERE is_deleted = FALSE
          AND D2_PEDIDO IS NOT NULL 
          AND TRIM(D2_PEDIDO) != ''
    ),
    l1_dedup AS (
        -- Point 3: Patient resolution via SL10X0 L1 (silver.venda_direta)
        SELECT 
            L1_FILIAL,
            L1_PEDRES,
            L1_PACIENT,
            L1_NOMPACI,
            ROW_NUMBER() OVER(
                PARTITION BY L1_FILIAL, L1_PEDRES 
                ORDER BY extraction_timestamp DESC
            ) as rn
        FROM silver.venda_direta
        WHERE is_deleted = FALSE 
          AND L1_PEDRES IS NOT NULL 
          AND TRIM(L1_PEDRES) != ''
    )
    SELECT
        CASE
            WHEN p.company_id = '01' THEN '1'
            WHEN p.company_id = '03' THEN '3'
            WHEN p.company_id = '05' THEN '5'
            WHEN p.company_id = '06' THEN '6'
            WHEN p.company_id = '07' THEN '7'
            ELSE 'Unknown'
        END AS "Grp",
        TRY_CAST(p.C5_FILIAL AS INTEGER) AS "Filial",
        TRY_CAST(p.C5_NUM AS INTEGER) AS "Pedido",
        TRY_CAST(p.C5_ORCRES AS INTEGER) AS "Orcamento",
        TRY_CAST(p.C5_CLIENTE AS INTEGER) AS "Cliente",
        c_cli_cli.A1_NOME AS "Nome",
        c_cli_cli.A1_CGC AS CPF,
        TRY_CAST(-1 AS INTEGER) AS prontuario,
        -- Point 3: Map L1_PACIENT with fallback to C6_CLI
        TRY_CAST(COALESCE(l1.L1_PACIENT, p.C6_CLI) AS INTEGER) AS "Paciente",
        -- Point 3: Map L1_NOMPACI with fallback to A1_NOME
        COALESCE(l1.L1_NOMPACI, c_cli_pat.A1_NOME) AS "Nome Paciente",
        TRY_CAST(p.C5_VEND1 AS INTEGER) AS "Medico",
        v.A3_NOME AS "Nome Medico",
        CAST(p.C5_EMISSAO AS TIMESTAMP) AS "Emissao",
        COALESCE(TRY_CAST(p.C6_NOTA AS INTEGER), TRY_CAST(n_item.F2_DOC AS INTEGER)) AS "Numero",
        COALESCE(p.C6_SERIE, n_item.F2_SERIE) AS "NFSe",
        TRY_CAST(prod.B1_GRUPO AS INTEGER) AS "Grupo",
        p.C6_PRODUTO AS "Produt",
        COALESCE(prod.B1_DESC, p.C6_DESCRI) AS "Descricao",
        p.C6_QTDVEN AS "Quantidade",
        p.C6_PRCVEN AS "Vlr.Unit",
        p.C6_VALOR AS "Vlr.Mercadoria",
        0.0 AS "Vlr.ISS",
        TRY_CAST(p.C6_VALDESC AS DOUBLE) AS "Vlr.Desconto",
        0.0 AS "Vlr.Custo",
        TRY_CAST(p.C6_PRUNIT AS DOUBLE) AS "Vlr.Custo Unit",
        0.0 AS "Vlr.Comissao",
        p.C6_VALOR AS "Total",
        CAST(NULL AS DOUBLE) AS "Vlr.Bruto",
        TRY_CAST(p.C6_PRUNIT AS DOUBLE) AS "Prc.Venda",
        0.0 AS "Ult.Preco"
    FROM silver.pedidos p
    LEFT JOIN silver.clientes c_cli_cli
        ON p.C5_CLIENTE = c_cli_cli.A1_COD AND p.C5_LOJACLI = c_cli_cli.A1_LOJA
    LEFT JOIN l1_dedup l1
        ON p.C5_FILIAL = l1.L1_FILIAL AND p.C5_NUM = l1.L1_PEDRES AND l1.rn = 1
    LEFT JOIN silver.clientes c_cli_pat
        ON p.C6_CLI = c_cli_pat.A1_COD AND p.C5_LOJACLI = c_cli_pat.A1_LOJA
    LEFT JOIN silver.produtos prod
        ON p.C6_PRODUTO = prod.B1_COD
    LEFT JOIN silver.vendedores v
        ON p.C5_VEND1 = v.A3_COD
    LEFT JOIN notas_dedup n_item
        ON p.C5_NUM = n_item.D2_PEDIDO 
       AND p.C5_FILIAL = n_item.F2_FILIAL 
       AND p.C6_PRODUTO = n_item.D2_COD 
       AND n_item.rn = 1

    WHERE p.is_deleted = FALSE
      -- Point 2: Budget Identifier Filtering (C5_ORCRES <> '')
      AND p.C5_ORCRES IS NOT NULL AND TRIM(p.C5_ORCRES) != '';
    """

    con.execute(query)
    count = con.execute("SELECT COUNT(*) FROM gold.protheus_pedidos_a_faturar").fetchone()[0]
    logger.info(f"Created gold.protheus_pedidos_a_faturar with {count:,} rows")

    # Log patient coverage stats
    patient_stats = con.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(CASE WHEN "Paciente" != "Cliente" THEN 1 END) as patient_differs_from_client,
            COUNT(CASE WHEN "Nome Paciente" IS NOT NULL AND "Nome Paciente" != '' THEN 1 END) as has_patient_name
        FROM gold.protheus_pedidos_a_faturar
    """).fetchone()
    pct_differs = (patient_stats[1] / patient_stats[0] * 100) if patient_stats[0] else 0.0
    pct_has_name = (patient_stats[2] / patient_stats[0] * 100) if patient_stats[0] else 0.0
    logger.info(f"  Patient coverage: {patient_stats[1]:,}/{patient_stats[0]:,} rows where Patient != Client ({pct_differs:.1f}%)")
    logger.info(f"  Patient name coverage: {patient_stats[2]:,}/{patient_stats[0]:,} rows with patient name ({pct_has_name:.1f}%)")


def update_prontuario_column_pedidos(con):
    logger.info("Updating prontuario column in gold.protheus_pedidos_a_faturar using Strategy L matching (Paciente & Cliente)...")

    # 1. First run: Paciente
    logger.info("Run 1 (pedidos): Matching via Paciente columns...")
    find_prontuarios(
        source_con=con,
        clinisys_db_path=CLINISYS_DB_PATH,
        source_schema='gold',
        source_table='protheus_pedidos_a_faturar',
        id_col='Paciente',
        name_col='Nome Paciente',
        birthdate_col=None,
        cpf_col=None,
        label='pedidos_paciente',
        suffix='',
    )

    # 2. Second run: Cliente (matching remaining unmatched)
    logger.info("Run 2 (pedidos): Matching via Cliente columns...")
    find_prontuarios(
        source_con=con,
        clinisys_db_path=CLINISYS_DB_PATH,
        source_schema='gold',
        source_table='protheus_pedidos_a_faturar',
        id_col='Cliente',
        name_col='Nome',
        birthdate_col=None,
        cpf_col='CPF',
        label='pedidos_cliente',
        suffix='',
    )

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


def create_gold_vendas_consolidadas_table(con):
    logger.info("Combining silver.venda_direta and gold.protheus_pedidos_a_faturar into gold.protheus_vendas_consolidadas...")

    con.execute("DROP TABLE IF EXISTS gold.protheus_vendas_consolidadas")

    query = """
    CREATE TABLE gold.protheus_vendas_consolidadas AS
    WITH common_window AS (
        SELECT 
            GREATEST(
                (SELECT MIN(L1_EMISSAO) FROM silver.venda_direta WHERE is_deleted = FALSE),
                (SELECT MIN(CAST("Emissao" AS DATE)) FROM gold.protheus_pedidos_a_faturar)
            ) AS start_date,
            LEAST(
                (SELECT MAX(L1_EMISSAO) FROM silver.venda_direta WHERE is_deleted = FALSE),
                (SELECT MAX(CAST("Emissao" AS DATE)) FROM gold.protheus_pedidos_a_faturar)
            ) AS end_date
    ),
    venda_direta_rows AS (
        SELECT 
            'VENDA_DIRETA' AS origem,
            CASE
                WHEN v.company_id = '01' THEN '1'
                WHEN v.company_id = '03' THEN '3'
                WHEN v.company_id = '05' THEN '5'
                WHEN v.company_id = '06' THEN '6'
                WHEN v.company_id = '07' THEN '7'
                ELSE 'Unknown'
            END AS grp,
            CASE
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
            TRY_CAST(v.L1_FILIAL AS INTEGER) AS filial,
            TRY_CAST(v.L1_PEDRES AS INTEGER) AS pedido,
            TRY_CAST(COALESCE(v.L1_ORCRES, v.L1_NUM) AS INTEGER) AS orcamento,
            TRY_CAST(v.L1_CLIENTE AS INTEGER) AS cliente_id,
            c_cli.A1_NOME AS nome_cliente,
            c_cli.A1_CGC AS cpf,
            TRY_CAST(-1 AS INTEGER) AS prontuario,
            TRY_CAST(COALESCE(v.L1_PACIENT, c_pac.A1_COD) AS INTEGER) AS paciente_id,
            COALESCE(v.L1_NOMPACI, c_pac.A1_NOME) AS nome_paciente,
            TRY_CAST(v.L1_VEND AS INTEGER) AS medico_id,
            vend.A3_NOME AS nome_medico,
            CAST(v.L1_EMISSAO AS TIMESTAMP) AS dt_emissao,
            MONTH(v.L1_EMISSAO) AS mes,
            YEAR(v.L1_EMISSAO) AS ano,
            TRY_CAST(COALESCE(v.L1_DOC, v.L2_DOC) AS INTEGER) AS num_nota,
            COALESCE(v.L1_SERIE, v.L2_SERIE) AS serie_nota,
            v.L2_PRODUTO AS produto_id,
            TRY_CAST(prod.B1_GRUPO AS INTEGER) AS grupo_produto,
            COALESCE(prod.B1_DESC, v.L2_DESCRI) AS descricao_produto,
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
            TRY_CAST(v.L2_VLRITEM AS DOUBLE) AS valor_total,
            v.L1_FORMPG AS forma_pagamento,
            v.L1_CONDPG AS condicao_pagamento,
            v.L1_OPERADO AS operador,
            v.extraction_timestamp AS extraction_timestamp
        FROM silver.venda_direta v
        LEFT JOIN silver.clientes c_cli
            ON v.L1_CLIENTE = c_cli.A1_COD AND v.L1_LOJA = c_cli.A1_LOJA
        LEFT JOIN silver.clientes c_pac
            ON v.L1_PACIENT = c_pac.A1_COD AND c_pac.A1_LOJA = '01'
        LEFT JOIN silver.produtos prod
            ON v.L2_PRODUTO = prod.B1_COD
        LEFT JOIN silver.vendedores vend
            ON v.L1_VEND = vend.A3_COD
        WHERE v.is_deleted = FALSE
          AND v.L1_EMISSAO BETWEEN (SELECT start_date FROM common_window) AND (SELECT end_date FROM common_window)
    ),
    pedidos_rows AS (
        SELECT 
            'PEDIDO_A_FATURAR' AS origem,
            p."Grp" AS grp,
            CASE
                WHEN p."Grp" = '1' AND p."Filial" IN (10101, 10150) THEN 'Ibirapuera'
                WHEN p."Grp" = '1' AND p."Filial" IN (10155, 10104, 10106) THEN 'Vila Mariana'
                WHEN p."Grp" = '3' AND p."Filial" = 30101 THEN 'Campinas'
                WHEN p."Grp" = '6' AND p."Filial" = 60101 THEN 'Pro Fiv'
                WHEN p."Grp" = '5' AND p."Filial" = 101 THEN 'Belo Horizonte'
                WHEN p."Grp" = '7' AND p."Filial" IN (10101, 20101) THEN 'Salvador - Cenafert'
                WHEN p."Grp" = '7' AND p."Filial" IN (30101) THEN 'FIV Brasilia'
                WHEN p."Grp" = '7' AND p."Filial" IN (40101, 40102) THEN 'Rio de Janeiro'
                ELSE 'Unknown Unit (' || COALESCE(p."Grp", '') || ', ' || COALESCE(CAST(p."Filial" AS VARCHAR), '') || ')'
            END AS unidade,
            p."Filial" AS filial,
            p."Pedido" AS pedido,
            p."Orcamento" AS orcamento,
            p."Cliente" AS cliente_id,
            p."Nome" AS nome_cliente,
            p."CPF" AS cpf,
            p.prontuario AS prontuario,
            p."Paciente" AS paciente_id,
            p."Nome Paciente" AS nome_paciente,
            p."Medico" AS medico_id,
            p."Nome Medico" AS nome_medico,
            p."Emissao" AS dt_emissao,
            MONTH(p."Emissao") AS mes,
            YEAR(p."Emissao") AS ano,
            p."Numero" AS num_nota,
            p."NFSe" AS serie_nota,
            p."Produt" AS produto_id,
            p."Grupo" AS grupo_produto,
            p."Descricao" AS descricao_produto,
            prod.B1_ZDGEREN AS descricao_gerencial,
            prod.B1_ZMAPING AS descricao_mapping_actividad,
            TRY_CAST(prod.B1_ZCICLOS AS INTEGER) AS ciclos,
            p."Quantidade" AS quantidade,
            p."Vlr.Unit" AS valor_unitario,
            p."Vlr.Mercadoria" AS valor_mercadoria,
            COALESCE(p."Vlr.Desconto", 0.0) AS valor_desconto,
            COALESCE(p."Vlr.Custo", 0.0) AS valor_custo,
            COALESCE(p."Vlr.Custo Unit", 0.0) AS valor_custo_unit,
            COALESCE(p."Vlr.ISS", 0.0) AS valor_iss,
            COALESCE(p."Vlr.Comissao", 0.0) AS valor_comissao,
            p."Total" AS valor_total,
            CAST(NULL AS VARCHAR) AS forma_pagamento,
            CAST(NULL AS VARCHAR) AS condicao_pagamento,
            CAST(NULL AS VARCHAR) AS operador,
            CURRENT_TIMESTAMP::VARCHAR AS extraction_timestamp
        FROM gold.protheus_pedidos_a_faturar p
        LEFT JOIN silver.produtos prod
            ON p."Produt" = prod.B1_COD
        WHERE CAST(p."Emissao" AS DATE) BETWEEN (SELECT start_date FROM common_window) AND (SELECT end_date FROM common_window)
    ),
    unioned AS (
        SELECT * FROM venda_direta_rows
        UNION ALL
        SELECT * FROM pedidos_rows
    )
    SELECT 
        ROW_NUMBER() OVER (ORDER BY dt_emissao DESC, filial ASC, orcamento DESC, pedido DESC) AS line_number,
        *
    FROM unioned;
    """

    con.execute(query)
    count = con.execute("SELECT COUNT(*) FROM gold.protheus_vendas_consolidadas").fetchone()[0]
    logger.info(f"Created gold.protheus_vendas_consolidadas with {count:,} rows")


def update_prontuario_column_vendas_consolidadas(con):
    logger.info("Updating prontuario column in gold.protheus_vendas_consolidadas using Strategy L matching (Paciente & Cliente)...")

    # 1. First run: Paciente
    logger.info("Run 1 (vendas_consolidadas): Matching via paciente columns...")
    find_prontuarios(
        source_con=con,
        clinisys_db_path=CLINISYS_DB_PATH,
        source_schema='gold',
        source_table='protheus_vendas_consolidadas',
        id_col='paciente_id',
        name_col='nome_paciente',
        birthdate_col=None,
        cpf_col=None,
        label='vendas_consolidadas_paciente',
        suffix='',
    )

    # 2. Second run: Cliente (matching remaining unmatched)
    logger.info("Run 2 (vendas_consolidadas): Matching via cliente columns...")
    find_prontuarios(
        source_con=con,
        clinisys_db_path=CLINISYS_DB_PATH,
        source_schema='gold',
        source_table='protheus_vendas_consolidadas',
        id_col='cliente_id',
        name_col='nome_cliente',
        birthdate_col=None,
        cpf_col='cpf',
        label='vendas_consolidadas_cliente',
        suffix='',
    )

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


def main():
    logger.info("=== PROTHEUS SILVER TO GOLD CONSOLIDATION STARTED ===")
    logger.info(f"Target Database: {DUCKDB_PATH}")

    try:
        with duckdb.connect(DUCKDB_PATH) as con:
            con.execute("CREATE SCHEMA IF NOT EXISTS gold")
            create_gold_table(con)
            update_prontuario_column(con)
            create_gold_pedidos_a_faturar_table(con)
            update_prontuario_column_pedidos(con)
            create_gold_vendas_consolidadas_table(con)
            update_prontuario_column_vendas_consolidadas(con)
            logger.info("=== PROTHEUS SILVER TO GOLD CONSOLIDATION FINISHED SUCCESSFUL ===")
    except Exception as e:
        logger.error(f"Gold Consolidation Failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()

