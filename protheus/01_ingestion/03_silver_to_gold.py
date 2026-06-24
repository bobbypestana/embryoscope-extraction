#!/usr/bin/env python3
"""
Protheus Silver to Gold Consolidation Script
Combines silver.notas, silver.clientes, silver.produtos, silver.vendedores, and silver.tes
to build gold.protheus_mesclada_vendas. Maps customer and patient identifiers directly
from Protheus ERP tables, while resolving the couple chart ID (prontuario) via tiered Clinisys matching.
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
    logger.info("Combining Silver tables into gold.protheus_mesclada_vendas...")

    con.execute("DROP TABLE IF EXISTS gold.protheus_mesclada_vendas")

    query = """
    CREATE TABLE gold.protheus_mesclada_vendas AS
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
    logger.info("Updating prontuario column using Strategy L matching (Paciente & Cliente)...")

    # 1. First run: Paciente
    logger.info("Run 1: Matching via Paciente columns...")
    find_prontuarios(
        source_con=con,
        clinisys_db_path=CLINISYS_DB_PATH,
        source_schema='gold',
        source_table='protheus_mesclada_vendas',
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
        source_table='protheus_mesclada_vendas',
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
        FROM gold.protheus_mesclada_vendas
    """).fetchone()
    rate = stats[1] / stats[0] * 100 if stats[0] else 0.0
    logger.info(f"Final prontuario matching stats: Total={stats[0]:,}, Matched={stats[1]:,}, Unmatched={stats[2]:,}, Rate={rate:.2f}%")


def main():
    logger.info("=== PROTHEUS SILVER TO GOLD CONSOLIDATION STARTED ===")
    logger.info(f"Target Database: {DUCKDB_PATH}")

    try:
        with duckdb.connect(DUCKDB_PATH) as con:
            con.execute("CREATE SCHEMA IF NOT EXISTS gold")
            create_gold_table(con)
            update_prontuario_column(con)
            logger.info("=== PROTHEUS SILVER TO GOLD CONSOLIDATION FINISHED SUCCESSFUL ===")
    except Exception as e:
        logger.error(f"Gold Consolidation Failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
