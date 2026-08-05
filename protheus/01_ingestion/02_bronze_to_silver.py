#!/usr/bin/env python3
"""
Protheus Bronze to Silver Promotion Script
Promotes ALL columns from Bronze, applies type casts on known fields,
deduplicates by business PK, and flags deleted records for full-load tables.

Strategy:
- notas: incremental; dedup by (F2_FILIAL, F2_DOC, F2_SERIE, D2_ITEM), is_deleted always FALSE
- full-load tables (tes, produtos, clientes, vendedores): all PKs from a single batch share
  one extraction_timestamp; is_deleted=TRUE for any PK whose latest timestamp < MAX(timestamp),
  meaning it was present in an earlier run but absent from the most recent one.
"""

import os
import ast
import yaml
import logging
import pandas as pd
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


def promote_notas(con):
    logger.info("Promoting table 'notas' from Bronze to Silver...")

    exists = con.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'bronze' AND table_name = 'notas'
    """).fetchone()[0]

    if not exists:
        logger.error("Bronze table 'bronze.notas' not found. Ingestion must run first.")
        return

    # Promote ALL columns from bronze, only replacing fields that need type casting.
    # DuckDB's SELECT * REPLACE (...) syntax re-selects all columns but overrides specific ones.
    # We filter out records that have been flagged as deleted in the source.
    query = """
    CREATE OR REPLACE TABLE silver.notas AS
    SELECT * REPLACE (
        CAST(try_strptime(F2_EMISSAO, '%Y%m%d') AS DATE) AS F2_EMISSAO,
        TRY_CAST(D2_QUANT  AS DOUBLE) AS D2_QUANT,
        TRY_CAST(D2_PRCVEN AS DOUBLE) AS D2_PRCVEN,
        TRY_CAST(D2_TOTAL  AS DOUBLE) AS D2_TOTAL,
        TRY_CAST(D2_DESC   AS DOUBLE) AS D2_DESC
    ),
    FALSE AS is_deleted
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY company_id, F2_FILIAL, F2_DOC, F2_SERIE, D2_ITEM
                   ORDER BY extraction_timestamp DESC
               ) AS rn
        FROM bronze.notas
        WHERE COALESCE(is_deleted, 'FALSE') = 'FALSE'
    )
    WHERE rn = 1;
    """

    con.execute(query)
    count = con.execute("SELECT COUNT(*) FROM silver.notas").fetchone()[0]
    logger.info(f"Successfully promoted 'notas' to Silver. Rows: {count:,}")


def promote_full_load(con, name, pk_cols, keep_all=False):
    """
    Generic promotion for full-load tables. Promotes ALL bronze columns.
    is_deleted: TRUE if the record's extraction_timestamp < MAX(extraction_timestamp)
    in bronze — meaning it was present in an older run but absent from the latest batch.
    If keep_all=True, is_deleted is always set to FALSE (retaining all history, e.g. for clientes).
    This works correctly because 01_source_to_bronze.py assigns a single batch timestamp
    to all pages of a given full-load run.
    """
    logger.info(f"Promoting table '{name}' from Bronze to Silver...")

    exists = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'bronze' AND table_name = '{name}'
    """).fetchone()[0]

    if not exists:
        logger.error(f"Bronze table 'bronze.{name}' not found.")
        return

    pk_partition = ", ".join(pk_cols)

    if keep_all:
        is_deleted_expr = "FALSE"
    else:
        is_deleted_expr = "extraction_timestamp < (SELECT max_ts FROM latest_batch)"

    query = f"""
    CREATE OR REPLACE TABLE silver.{name} AS
    WITH latest_batch AS (
        SELECT MAX(extraction_timestamp) AS max_ts FROM bronze.{name}
    ),
    deduped AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY {pk_partition}
                   ORDER BY extraction_timestamp DESC
               ) AS rn
        FROM bronze.{name}
    )
    SELECT
        * EXCLUDE (rn),
        CASE WHEN {is_deleted_expr}
             THEN TRUE ELSE FALSE END AS is_deleted
    FROM deduped
    WHERE rn = 1;
    """

    con.execute(query)
    count = con.execute(f"SELECT COUNT(*) FROM silver.{name}").fetchone()[0]
    deleted_count = con.execute(f"SELECT COUNT(*) FROM silver.{name} WHERE is_deleted = TRUE").fetchone()[0]
    logger.info(f"Successfully promoted '{name}' to Silver. Rows: {count:,} (Deleted: {deleted_count:,})")

def promote_pedidos(con):
    logger.info("Promoting table 'pedidos' from Bronze to Silver...")

    exists = con.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'bronze' AND table_name = 'pedidos'
    """).fetchone()[0]

    if not exists:
        logger.error("Bronze table 'bronze.pedidos' not found.")
        return

    # Promote ALL columns from bronze, only replacing fields that need type casting.
    # Deduplicate by business primary keys: company_id, C5_FILIAL, C5_NUM, C6_ITEM
    # We filter out records that have been flagged as deleted in the source.
    query = """
    CREATE OR REPLACE TABLE silver.pedidos AS
    SELECT * REPLACE (
        CAST(try_strptime(C5_EMISSAO, '%Y%m%d') AS DATE) AS C5_EMISSAO,
        TRY_CAST(C6_QTDVEN  AS DOUBLE) AS C6_QTDVEN,
        TRY_CAST(C6_PRCVEN  AS DOUBLE) AS C6_PRCVEN,
        TRY_CAST(C6_VALOR   AS DOUBLE) AS C6_VALOR,
        TRY_CAST(C6_DESCONT AS DOUBLE) AS C6_DESCONT
    ),
    FALSE AS is_deleted
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY company_id, C5_FILIAL, C5_NUM, C6_ITEM
                   ORDER BY extraction_timestamp DESC
               ) AS rn
        FROM bronze.pedidos
        WHERE COALESCE(is_deleted, 'FALSE') = 'FALSE'
    )
    WHERE rn = 1;
    """

    con.execute(query)
    count = con.execute("SELECT COUNT(*) FROM silver.pedidos").fetchone()[0]
    logger.info(f"Successfully promoted 'pedidos' to Silver. Rows: {count:,}")


def promote_venda_direta(con):
    logger.info("Promoting table 'venda_direta' from Bronze to Silver...")

    exists = con.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'bronze' AND table_name = 'venda_direta'
    """).fetchone()[0]

    if not exists:
        logger.error("Bronze table 'bronze.venda_direta' not found.")
        return

    # Promote ALL columns from bronze, only replacing fields that need type casting.
    # Deduplicate by business primary keys: company_id, L1_FILIAL, L1_NUM, L2_ITEM
    # We filter out records that have been flagged as deleted in the source.
    query = """
    CREATE OR REPLACE TABLE silver.venda_direta AS
    SELECT * REPLACE (
        CAST(try_strptime(L1_EMISSAO, '%Y%m%d') AS DATE) AS L1_EMISSAO,
        TRY_CAST(L2_QUANT   AS DOUBLE) AS L2_QUANT,
        TRY_CAST(L2_VRUNIT  AS DOUBLE) AS L2_VRUNIT,
        TRY_CAST(L2_VLRITEM AS DOUBLE) AS L2_VLRITEM,
        TRY_CAST(L2_DESC    AS DOUBLE) AS L2_DESC
    ),
    FALSE AS is_deleted
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY company_id, L1_FILIAL, L1_NUM, L2_ITEM
                   ORDER BY extraction_timestamp DESC
               ) AS rn
        FROM bronze.venda_direta
        WHERE COALESCE(is_deleted, 'FALSE') = 'FALSE'
    )
    WHERE rn = 1;
    """

    con.execute(query)
    count = con.execute("SELECT COUNT(*) FROM silver.venda_direta").fetchone()[0]
    logger.info(f"Successfully promoted 'venda_direta' to Silver. Rows: {count:,}")


def promote_pagamentos(con):
    logger.info("Promoting 'pagamentos' from bronze.venda_direta PAGAMENTO column to silver...")

    exists = con.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'bronze' AND table_name = 'venda_direta'
    """).fetchone()[0]
    if not exists:
        logger.error("Bronze table 'bronze.venda_direta' not found. Skipping pagamentos promotion.")
        return

    # Use DuckDB JSON functionality to unnest the array of objects in the PAGAMENTO column.
    query = """
    CREATE OR REPLACE TABLE silver.pagamentos AS
    WITH parsed_payments AS (
        SELECT 
            company_id,
            extraction_timestamp,
            -- Unnest the JSON list into individual row items
            UNNEST(
                FROM_JSON(
                    PAGAMENTO,
                    '["JSON"]'
                )
            ) AS payment_json
        FROM bronze.venda_direta
        WHERE PAGAMENTO IS NOT NULL
          AND TRIM(PAGAMENTO) NOT IN ('', 'None', '[]')
          AND COALESCE(is_deleted, 'FALSE') = 'FALSE'
    ),
    extracted_payments AS (
        SELECT
            company_id,
            extraction_timestamp,
            JSON_EXTRACT_STRING(payment_json, '$.L4_FILIAL') AS L4_FILIAL,
            JSON_EXTRACT_STRING(payment_json, '$.L4_NUM') AS L4_NUM,
            CAST(TRY_STRPTIME(JSON_EXTRACT_STRING(payment_json, '$.L4_DATA'), '%Y%m%d') AS DATE) AS L4_DATA,
            TRY_CAST(JSON_EXTRACT_STRING(payment_json, '$.L4_VALOR') AS DOUBLE) AS L4_VALOR,
            JSON_EXTRACT_STRING(payment_json, '$.L4_FORMA') AS L4_FORMA,
            JSON_EXTRACT_STRING(payment_json, '$.L4_ADMINIS') AS L4_ADMINIS,
            JSON_EXTRACT_STRING(payment_json, '$.L4_NUMCART') AS L4_NUMCART,
            JSON_EXTRACT_STRING(payment_json, '$.L4_AGENCIA') AS L4_AGENCIA,
            JSON_EXTRACT_STRING(payment_json, '$.L4_CONTA') AS L4_CONTA,
            JSON_EXTRACT_STRING(payment_json, '$.L4_RG') AS L4_RG,
            JSON_EXTRACT_STRING(payment_json, '$.L4_TELEFON') AS L4_TELEFON,
            JSON_EXTRACT_STRING(payment_json, '$.L4_OBS') AS L4_OBS,
            JSON_EXTRACT_STRING(payment_json, '$.L4_TERCEIR') AS L4_TERCEIR,
            JSON_EXTRACT_STRING(payment_json, '$.L4_SITUA') AS L4_SITUA,
            JSON_EXTRACT_STRING(payment_json, '$.L4_DATATEF') AS L4_DATATEF,
            JSON_EXTRACT_STRING(payment_json, '$.L4_HORATEF') AS L4_HORATEF,
            JSON_EXTRACT_STRING(payment_json, '$.L4_DOCTEF') AS L4_DOCTEF,
            JSON_EXTRACT_STRING(payment_json, '$.L4_AUTORIZ') AS L4_AUTORIZ,
            JSON_EXTRACT_STRING(payment_json, '$.L4_DATCANC') AS L4_DATCANC,
            JSON_EXTRACT_STRING(payment_json, '$.L4_HORCANC') AS L4_HORCANC,
            JSON_EXTRACT_STRING(payment_json, '$.L4_DOCCANC') AS L4_DOCCANC,
            JSON_EXTRACT_STRING(payment_json, '$.L4_INSTITU') AS L4_INSTITU,
            JSON_EXTRACT_STRING(payment_json, '$.L4_NSUTEF') AS L4_NSUTEF,
            JSON_EXTRACT_STRING(payment_json, '$.L4_TIPCART') AS L4_TIPCART,
            TRY_CAST(JSON_EXTRACT_STRING(payment_json, '$.L4_MOEDA') AS DOUBLE) AS L4_MOEDA,
            JSON_EXTRACT_STRING(payment_json, '$.L4_MESACTA') AS L4_MESACTA,
            JSON_EXTRACT_STRING(payment_json, '$.L4_ANOACTA') AS L4_ANOACTA,
            JSON_EXTRACT_STRING(payment_json, '$.L4_TIPOCHQ') AS L4_TIPOCHQ,
            JSON_EXTRACT_STRING(payment_json, '$.L4_CGC') AS L4_CGC,
            JSON_EXTRACT_STRING(payment_json, '$.L4_NOMECLI') AS L4_NOMECLI,
            JSON_EXTRACT_STRING(payment_json, '$.L4_SERCHQ') AS L4_SERCHQ,
            JSON_EXTRACT_STRING(payment_json, '$.L4_COMP') AS L4_COMP,
            JSON_EXTRACT_STRING(payment_json, '$.L4_ORIGEM') AS L4_ORIGEM,
            JSON_EXTRACT_STRING(payment_json, '$.L4_FORMPG') AS L4_FORMPG,
            JSON_EXTRACT_STRING(payment_json, '$.L4_VENDTEF') AS L4_VENDTEF,
            JSON_EXTRACT_STRING(payment_json, '$.L4_FORMAID') AS L4_FORMAID,
            JSON_EXTRACT_STRING(payment_json, '$.L4_PARCTEF') AS L4_PARCTEF,
            TRY_CAST(JSON_EXTRACT_STRING(payment_json, '$.L4_TROCO') AS DOUBLE) AS L4_TROCO,
            JSON_EXTRACT_STRING(payment_json, '$.L4_ITEM') AS L4_ITEM,
            JSON_EXTRACT_STRING(payment_json, '$.L4_ESTORN') AS L4_ESTORN,
            JSON_EXTRACT_STRING(payment_json, '$.L4_OPERAES') AS L4_OPERAES,
            JSON_EXTRACT_STRING(payment_json, '$.L4_SERPDV') AS L4_SERPDV,
            JSON_EXTRACT_STRING(payment_json, '$.L4_PAFMD5') AS L4_PAFMD5,
            JSON_EXTRACT_STRING(payment_json, '$.L4_DOC') AS L4_DOC,
            JSON_EXTRACT_STRING(payment_json, '$.L4_CONTDOC') AS L4_CONTDOC,
            JSON_EXTRACT_STRING(payment_json, '$.L4_CONTONF') AS L4_CONTONF,
            TRY_CAST(JSON_EXTRACT_STRING(payment_json, '$.L4_DESPRC') AS DOUBLE) AS L4_DESPRC,
            JSON_EXTRACT_STRING(payment_json, '$.L4_BANPRC') AS L4_BANPRC,
            TRY_CAST(JSON_EXTRACT_STRING(payment_json, '$.L4_ACRSFIN') AS DOUBLE) AS L4_ACRSFIN,
            JSON_EXTRACT_STRING(payment_json, '$.L4_PROCFID') AS L4_PROCFID,
            JSON_EXTRACT_STRING(payment_json, '$.L4_NUMCFID') AS L4_NUMCFID,
            JSON_EXTRACT_STRING(payment_json, '$.L4_CONHTL') AS L4_CONHTL,
            JSON_EXTRACT_STRING(payment_json, '$.L4_CODVP') AS L4_CODVP,
            TRY_CAST(JSON_EXTRACT_STRING(payment_json, '$.L4_DESCMN') AS DOUBLE) AS L4_DESCMN,
            JSON_EXTRACT_STRING(payment_json, '$.L4_BANDEIR') AS L4_BANDEIR,
            JSON_EXTRACT_STRING(payment_json, '$.L4_REDEAUT') AS L4_REDEAUT,
            JSON_EXTRACT_STRING(payment_json, '$.L4_IDPGVFP') AS L4_IDPGVFP,
            JSON_EXTRACT_STRING(payment_json, '$.L4_IDRSPFI') AS L4_IDRSPFI,
            JSON_EXTRACT_STRING(payment_json, '$.L4_IDCNAB') AS L4_IDCNAB,
            JSON_EXTRACT_STRING(payment_json, '$.L4_TRNID') AS L4_TRNID,
            JSON_EXTRACT_STRING(payment_json, '$.L4_TRNPCID') AS L4_TRNPCID,
            JSON_EXTRACT_STRING(payment_json, '$.L4_TRNEXID') AS L4_TRNEXID,
            TRY_CAST(JSON_EXTRACT_STRING(payment_json, '$.L4_ACRCART') AS DOUBLE) AS L4_ACRCART
        FROM parsed_payments
    )
    SELECT * EXCLUDE(rn)
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY company_id, L4_FILIAL, L4_NUM, COALESCE(L4_DOCTEF, ''), COALESCE(L4_AUTORIZ, '')
                   ORDER BY extraction_timestamp DESC
               ) AS rn
        FROM extracted_payments
    )
    WHERE rn = 1;
    """

    con.execute(query)
    count = con.execute("SELECT COUNT(*) FROM silver.pagamentos").fetchone()[0]
    logger.info(f"Successfully promoted 'pagamentos' to Silver. Rows: {count:,}")


def main():
    logger.info("=== PROTHEUS BRONZE TO SILVER PROMOTION STARTED ===")
    logger.info(f"Target Database: {DUCKDB_PATH}")

    try:
        with duckdb.connect(DUCKDB_PATH) as con:
            con.execute("CREATE SCHEMA IF NOT EXISTS silver")

            # Ensure is_deleted column exists in bronze tables to prevent promotion BinderExceptions
            for table_name in ["notas", "pedidos", "venda_direta"]:
                exists = con.execute(f"""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_schema = 'bronze' AND table_name = '{table_name}'
                """).fetchone()[0]
                if exists:
                    cols = {r[0] for r in con.execute(f"DESCRIBE bronze.{table_name}").fetchall()}
                    if "is_deleted" not in cols:
                        logger.info(f"Ensuring is_deleted column exists in bronze.{table_name}...")
                        con.execute(f"ALTER TABLE bronze.{table_name} ADD COLUMN is_deleted VARCHAR")
                        con.execute(f"UPDATE bronze.{table_name} SET is_deleted = 'FALSE'")

            # Rename legacy pedidos_venda → venda_direta if still present
            for schema in ["bronze", "silver"]:
                old_exists = con.execute(f"""
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema = '{schema}' AND table_name = 'pedidos_venda'
                """).fetchone()[0]
                new_exists = con.execute(f"""
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema = '{schema}' AND table_name = 'venda_direta'
                """).fetchone()[0]
                if old_exists and not new_exists:
                    logger.info(f"Renaming {schema}.pedidos_venda → {schema}.venda_direta...")
                    con.execute(f"ALTER TABLE {schema}.pedidos_venda RENAME TO venda_direta")
                elif old_exists and new_exists:
                    logger.warning(f"Both {schema}.pedidos_venda and {schema}.venda_direta exist — skipping rename of {schema}.pedidos_venda.")

            promote_notas(con)
            promote_pedidos(con)
            promote_venda_direta(con)
            promote_pagamentos(con)
            promote_full_load(con, "empresas",   ["M0_CODIGO", "M0_CODFIL"])
            promote_full_load(con, "tes",        ["F4_CODIGO"])
            promote_full_load(con, "produtos",   ["B1_COD"])
            promote_full_load(con, "clientes",   ["A1_COD", "A1_LOJA"], keep_all=True)
            promote_full_load(con, "vendedores", ["A3_COD"])
            logger.info("=== PROTHEUS BRONZE TO SILVER PROMOTION FINISHED SUCCESSFUL ===")
    except Exception as e:
        logger.error(f"Silver Promotion Failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
