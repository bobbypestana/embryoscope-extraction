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
    # is_deleted is always FALSE for notas — deleted lines are naturally absent from the API
    # date-range queries and won't appear in new extracts, so we don't need a flag here.
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


def main():
    logger.info("=== PROTHEUS BRONZE TO SILVER PROMOTION STARTED ===")
    logger.info(f"Target Database: {DUCKDB_PATH}")

    con = duckdb.connect(DUCKDB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")

    try:
        promote_notas(con)
        promote_full_load(con, "tes",        ["F4_CODIGO"])
        promote_full_load(con, "produtos",   ["B1_COD"])
        promote_full_load(con, "clientes",   ["A1_COD", "A1_LOJA"], keep_all=True)
        promote_full_load(con, "vendedores", ["A3_COD"])
        logger.info("=== PROTHEUS BRONZE TO SILVER PROMOTION FINISHED SUCCESSFUL ===")
    except Exception as e:
        logger.error(f"Silver Promotion Failed: {e}", exc_info=True)
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
