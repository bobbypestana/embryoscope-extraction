#!/usr/bin/env python3
"""
Protheus Bronze to Silver Promotion Script
Promotes ALL columns from Bronze (both Huntington 'bronze' and BH 'bronze_bh'),
applies type casts on known fields, deduplicates by business PK per instance,
appends with instance_id tag ('HUNTINGTON' vs 'BH') preserving source column names,
and generates an Unmapped Columns report.

Strategy:
- notas: incremental; dedup by (company_id, F2_FILIAL, F2_DOC, F2_SERIE, D2_ITEM), is_deleted always FALSE
- full-load tables (tes, produtos, clientes, vendedores, empresas):
  dedup by business PK per instance; is_deleted=TRUE for any PK whose extraction_timestamp < MAX(timestamp) within that instance.
- union: instances appended into silver.{table} via UNION ALL BY NAME.
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

    h_exists = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = 'notas'").fetchone()[0]
    bh_exists = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'bronze_bh' AND table_name = 'notas'").fetchone()[0]

    if not h_exists and not bh_exists:
        logger.error("Bronze table 'notas' not found in either bronze or bronze_bh.")
        return

    queries = []
    if h_exists:
        queries.append("""
        SELECT 'HUNTINGTON' AS instance_id, * EXCLUDE (rn) REPLACE (
            CAST(try_strptime(F2_EMISSAO, '%Y%m%d') AS DATE) AS F2_EMISSAO,
            TRY_CAST(D2_QUANT  AS DOUBLE) AS D2_QUANT,
            TRY_CAST(D2_PRCVEN AS DOUBLE) AS D2_PRCVEN,
            TRY_CAST(D2_TOTAL  AS DOUBLE) AS D2_TOTAL,
            TRY_CAST(D2_DESC   AS DOUBLE) AS D2_DESC,
            FALSE AS is_deleted
        )
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY company_id, F2_FILIAL, F2_DOC, F2_SERIE, D2_ITEM
                       ORDER BY extraction_timestamp DESC
                   ) AS rn
            FROM bronze.notas
            WHERE COALESCE(is_deleted, 'FALSE') = 'FALSE'
        )
        WHERE rn = 1
        """)

    if bh_exists:
        queries.append("""
        SELECT 'BH' AS instance_id, * EXCLUDE (rn) REPLACE (
            CAST(try_strptime(F2_EMISSAO, '%Y%m%d') AS DATE) AS F2_EMISSAO,
            TRY_CAST(D2_QUANT  AS DOUBLE) AS D2_QUANT,
            TRY_CAST(D2_PRCVEN AS DOUBLE) AS D2_PRCVEN,
            TRY_CAST(D2_TOTAL  AS DOUBLE) AS D2_TOTAL,
            TRY_CAST(D2_DESC   AS DOUBLE) AS D2_DESC,
            FALSE AS is_deleted
        )
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY company_id, F2_FILIAL, F2_DOC, F2_SERIE, D2_ITEM
                       ORDER BY extraction_timestamp DESC
                   ) AS rn
            FROM bronze_bh.notas
            WHERE COALESCE(is_deleted, 'FALSE') = 'FALSE'
        )
        WHERE rn = 1
        """)

    union_sql = " UNION ALL BY NAME ".join(queries)
    con.execute(f"CREATE OR REPLACE TABLE silver.notas AS {union_sql};")
    count = con.execute("SELECT COUNT(*) FROM silver.notas").fetchone()[0]
    logger.info(f"Successfully promoted 'notas' to Silver. Total rows: {count:,}")


def promote_full_load(con, name, pk_cols, keep_all=False):
    """
    Generic promotion for full-load tables from both bronze and bronze_bh.
    Deduplicates by business PK independently per instance.
    Soft-deletes records whose extraction_timestamp < MAX(extraction_timestamp) within its instance.
    Appends both instances into silver.{name} using UNION ALL BY NAME.
    """
    logger.info(f"Promoting full-load table '{name}' from Bronze to Silver...")

    h_exists = con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = '{name}'").fetchone()[0]
    bh_exists = con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'bronze_bh' AND table_name = '{name}'").fetchone()[0]

    if not h_exists and not bh_exists:
        logger.error(f"Bronze table '{name}' not found in either bronze or bronze_bh.")
        return

    pk_partition = ", ".join(pk_cols)
    queries = []

    if h_exists:
        is_del_expr = "FALSE" if keep_all else "extraction_timestamp < (SELECT max_ts FROM latest_batch)"
        queries.append(f"""
        WITH latest_batch AS (
            SELECT MAX(extraction_timestamp) AS max_ts FROM bronze.{name}
        ),
        deduped AS (
            SELECT 'HUNTINGTON' AS instance_id, *,
                   ROW_NUMBER() OVER (
                       PARTITION BY {pk_partition}
                       ORDER BY extraction_timestamp DESC
                   ) AS rn
            FROM bronze.{name}
        )
        SELECT
            * EXCLUDE (rn),
            CASE WHEN {is_del_expr}
                 THEN TRUE ELSE FALSE END AS is_deleted
        FROM deduped
        WHERE rn = 1
        """)

    if bh_exists:
        is_del_expr = "FALSE" if keep_all else "extraction_timestamp < (SELECT max_ts FROM latest_batch)"
        queries.append(f"""
        WITH latest_batch AS (
            SELECT MAX(extraction_timestamp) AS max_ts FROM bronze_bh.{name}
        ),
        deduped AS (
            SELECT 'BH' AS instance_id, *,
                   ROW_NUMBER() OVER (
                       PARTITION BY {pk_partition}
                       ORDER BY extraction_timestamp DESC
                   ) AS rn
            FROM bronze_bh.{name}
        )
        SELECT
            * EXCLUDE (rn),
            CASE WHEN {is_del_expr}
                 THEN TRUE ELSE FALSE END AS is_deleted
        FROM deduped
        WHERE rn = 1
        """)

    union_sql = " UNION ALL BY NAME ".join([f"SELECT * FROM ({q})" for q in queries])
    con.execute(f"CREATE OR REPLACE TABLE silver.{name} AS {union_sql};")
    count = con.execute(f"SELECT COUNT(*) FROM silver.{name}").fetchone()[0]
    deleted_count = con.execute(f"SELECT COUNT(*) FROM silver.{name} WHERE is_deleted = TRUE").fetchone()[0]
    logger.info(f"Successfully promoted '{name}' to Silver. Rows: {count:,} (Deleted: {deleted_count:,})")


def promote_pedidos(con):
    logger.info("Promoting table 'pedidos' from Bronze to Silver...")

    h_exists = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = 'pedidos'").fetchone()[0]
    bh_exists = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'bronze_bh' AND table_name = 'pedidos'").fetchone()[0]

    if not h_exists and not bh_exists:
        logger.error("Bronze table 'pedidos' not found in either bronze or bronze_bh.")
        return

    queries = []
    if h_exists:
        queries.append("""
        SELECT 'HUNTINGTON' AS instance_id, * EXCLUDE (rn) REPLACE (
            CAST(try_strptime(C5_EMISSAO, '%Y%m%d') AS DATE) AS C5_EMISSAO,
            TRY_CAST(C6_QTDVEN  AS DOUBLE) AS C6_QTDVEN,
            TRY_CAST(C6_PRCVEN  AS DOUBLE) AS C6_PRCVEN,
            TRY_CAST(C6_VALOR   AS DOUBLE) AS C6_VALOR,
            TRY_CAST(C6_DESCONT AS DOUBLE) AS C6_DESCONT,
            FALSE AS is_deleted
        )
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY company_id, C5_FILIAL, C5_NUM, C6_ITEM
                       ORDER BY extraction_timestamp DESC
                   ) AS rn
            FROM bronze.pedidos
            WHERE COALESCE(is_deleted, 'FALSE') = 'FALSE'
        )
        WHERE rn = 1
        """)

    if bh_exists:
        queries.append("""
        SELECT 'BH' AS instance_id, * EXCLUDE (rn) REPLACE (
            CAST(try_strptime(C5_EMISSAO, '%Y%m%d') AS DATE) AS C5_EMISSAO,
            TRY_CAST(C6_QTDVEN  AS DOUBLE) AS C6_QTDVEN,
            TRY_CAST(C6_PRCVEN  AS DOUBLE) AS C6_PRCVEN,
            TRY_CAST(C6_VALOR   AS DOUBLE) AS C6_VALOR,
            TRY_CAST(C6_DESCONT AS DOUBLE) AS C6_DESCONT,
            FALSE AS is_deleted
        )
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY company_id, C5_FILIAL, C5_NUM, C6_ITEM
                       ORDER BY extraction_timestamp DESC
                   ) AS rn
            FROM bronze_bh.pedidos
            WHERE COALESCE(is_deleted, 'FALSE') = 'FALSE'
        )
        WHERE rn = 1
        """)

    union_sql = " UNION ALL BY NAME ".join(queries)
    con.execute(f"CREATE OR REPLACE TABLE silver.pedidos AS {union_sql};")
    count = con.execute("SELECT COUNT(*) FROM silver.pedidos").fetchone()[0]
    logger.info(f"Successfully promoted 'pedidos' to Silver. Rows: {count:,}")


def promote_venda_direta(con):
    logger.info("Promoting table 'venda_direta' from Bronze to Silver...")

    h_exists = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = 'venda_direta'").fetchone()[0]
    bh_exists = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'bronze_bh' AND table_name = 'venda_direta'").fetchone()[0]

    if not h_exists and not bh_exists:
        logger.error("Bronze table 'venda_direta' not found in either bronze or bronze_bh.")
        return

    queries = []
    if h_exists:
        queries.append("""
        SELECT 'HUNTINGTON' AS instance_id, * EXCLUDE (rn) REPLACE (
            CAST(try_strptime(L1_EMISSAO, '%Y%m%d') AS DATE) AS L1_EMISSAO,
            TRY_CAST(L2_QUANT   AS DOUBLE) AS L2_QUANT,
            TRY_CAST(L2_VRUNIT  AS DOUBLE) AS L2_VRUNIT,
            TRY_CAST(L2_VLRITEM AS DOUBLE) AS L2_VLRITEM,
            TRY_CAST(L2_DESC    AS DOUBLE) AS L2_DESC,
            FALSE AS is_deleted
        )
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY company_id, L1_FILIAL, L1_NUM, L2_ITEM
                       ORDER BY extraction_timestamp DESC
                   ) AS rn
            FROM bronze.venda_direta
            WHERE COALESCE(is_deleted, 'FALSE') = 'FALSE'
        )
        WHERE rn = 1
        """)

    if bh_exists:
        queries.append("""
        SELECT 'BH' AS instance_id, * EXCLUDE (rn) REPLACE (
            CAST(try_strptime(L1_EMISSAO, '%Y%m%d') AS DATE) AS L1_EMISSAO,
            TRY_CAST(L2_QUANT   AS DOUBLE) AS L2_QUANT,
            TRY_CAST(L2_VRUNIT  AS DOUBLE) AS L2_VRUNIT,
            TRY_CAST(L2_VLRITEM AS DOUBLE) AS L2_VLRITEM,
            TRY_CAST(L2_DESC    AS DOUBLE) AS L2_DESC,
            FALSE AS is_deleted
        )
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY company_id, L1_FILIAL, L1_NUM, L2_ITEM
                       ORDER BY extraction_timestamp DESC
                   ) AS rn
            FROM bronze_bh.venda_direta
            WHERE COALESCE(is_deleted, 'FALSE') = 'FALSE'
        )
        WHERE rn = 1
        """)

    union_sql = " UNION ALL BY NAME ".join(queries)
    con.execute(f"CREATE OR REPLACE TABLE silver.venda_direta AS {union_sql};")
    count = con.execute("SELECT COUNT(*) FROM silver.venda_direta").fetchone()[0]
    logger.info(f"Successfully promoted 'venda_direta' to Silver. Rows: {count:,}")


def promote_pagamentos(con):
    logger.info("Promoting 'pagamentos' from venda_direta PAGAMENTO column to silver...")

    h_exists = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = 'venda_direta'").fetchone()[0]
    bh_exists = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'bronze_bh' AND table_name = 'venda_direta'").fetchone()[0]

    if not h_exists and not bh_exists:
        logger.error("Bronze table 'venda_direta' not found. Skipping pagamentos promotion.")
        return

    queries = []
    for schema_name, inst_id in [("bronze", "HUNTINGTON"), ("bronze_bh", "BH")]:
        exists = con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_name = 'venda_direta'").fetchone()[0]
        if not exists:
            continue

        queries.append(f"""
        WITH parsed_payments AS (
            SELECT 
                '{inst_id}' AS instance_id,
                company_id,
                extraction_timestamp,
                UNNEST(
                    FROM_JSON(
                        PAGAMENTO,
                        '["JSON"]'
                    )
                ) AS payment_json
            FROM {schema_name}.venda_direta
            WHERE PAGAMENTO IS NOT NULL
              AND TRIM(PAGAMENTO) NOT IN ('', 'None', '[]')
              AND COALESCE(is_deleted, 'FALSE') = 'FALSE'
        ),
        extracted_payments AS (
            SELECT
                instance_id,
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
                       PARTITION BY instance_id, company_id, L4_FILIAL, L4_NUM, COALESCE(L4_DOCTEF, ''), COALESCE(L4_AUTORIZ, '')
                       ORDER BY extraction_timestamp DESC
                   ) AS rn
            FROM extracted_payments
        )
        WHERE rn = 1
        """)

    union_sql = " UNION ALL BY NAME ".join([f"SELECT * FROM ({q})" for q in queries])
    con.execute(f"CREATE OR REPLACE TABLE silver.pagamentos AS {union_sql};")
    count = con.execute("SELECT COUNT(*) FROM silver.pagamentos").fetchone()[0]
    logger.info(f"Successfully promoted 'pagamentos' to Silver. Rows: {count:,}")


def generate_unmapped_columns_report(con):
    """
    Generates a Markdown audit report comparing schemas between bronze (Huntington)
    and bronze_bh (BH), identifying common columns and unmapped/exclusive columns.
    """
    tables = ["notas", "pedidos", "venda_direta", "produtos", "clientes", "vendedores", "tes", "empresas"]
    report_lines = [
        "# Unmapped & Schema Variance Report (Huntington vs BH)",
        f"\n**Generated At:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "\nThis report audits all columns ingested from the two Protheus instances (`bronze` vs `bronze_bh`).",
        "\n| Table | Total Common Cols | Exclusive to Huntington | Exclusive to BH |",
        "| :--- | :---: | :---: | :---: |"
    ]

    details_lines = ["\n## Detailed Column Breakdown per Table\n"]

    for t in tables:
        h_exists = con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = '{t}'").fetchone()[0]
        bh_exists = con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'bronze_bh' AND table_name = '{t}'").fetchone()[0]

        if not h_exists and not bh_exists:
            continue

        h_cols = set()
        if h_exists:
            h_cols = {r[0] for r in con.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'bronze' AND table_name = '{t}'").fetchall()}

        bh_cols = set()
        if bh_exists:
            bh_cols = {r[0] for r in con.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'bronze_bh' AND table_name = '{t}'").fetchall()}

        common = h_cols.intersection(bh_cols)
        only_h = sorted(list(h_cols - bh_cols))
        only_bh = sorted(list(bh_cols - h_cols))

        report_lines.append(f"| `{t}` | {len(common)} | {len(only_h)} | {len(only_bh)} |")

        details_lines.append(f"### Table: `{t}`")
        details_lines.append(f"- **Common Columns ({len(common)}):** Standard shared Protheus fields.")
        if only_h:
            details_lines.append(f"- ⚠️ **Exclusive to Huntington ({len(only_h)}):** `{', '.join(only_h)}`")
        else:
            details_lines.append(f"- ✅ **Exclusive to Huntington:** None")

        if only_bh:
            details_lines.append(f"- ℹ️ **Exclusive to BH ({len(only_bh)}):** `{', '.join(only_bh)}`")
        else:
            details_lines.append(f"- ✅ **Exclusive to BH:** None")
        details_lines.append("")

    full_report = "\n".join(report_lines) + "\n" + "\n".join(details_lines)
    report_path = os.path.join(SCRIPT_DIR, "unmapped_columns_report.md")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(full_report)
    logger.info(f"Unmapped columns report written to: {report_path}")


def main():
    logger.info("=== PROTHEUS BRONZE TO SILVER PROMOTION STARTED ===")
    logger.info(f"Target Database: {DUCKDB_PATH}")

    try:
        with duckdb.connect(DUCKDB_PATH) as con:
            con.execute("CREATE SCHEMA IF NOT EXISTS silver")

            # Ensure is_deleted column exists in bronze tables to prevent promotion BinderExceptions
            for schema in ["bronze", "bronze_bh"]:
                for table_name in ["notas", "pedidos", "venda_direta"]:
                    exists = con.execute(f"""
                        SELECT COUNT(*) FROM information_schema.tables 
                        WHERE table_schema = '{schema}' AND table_name = '{table_name}'
                    """).fetchone()[0]
                    if exists:
                        cols = {r[0] for r in con.execute(f"DESCRIBE {schema}.{table_name}").fetchall()}
                        if "is_deleted" not in cols:
                            logger.info(f"Ensuring is_deleted column exists in {schema}.{table_name}...")
                            con.execute(f"ALTER TABLE {schema}.{table_name} ADD COLUMN is_deleted VARCHAR")
                            con.execute(f"UPDATE {schema}.{table_name} SET is_deleted = 'FALSE'")

            # Rename legacy pedidos_venda -> venda_direta if still present
            for schema in ["bronze", "bronze_bh", "silver"]:
                old_exists = con.execute(f"""
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema = '{schema}' AND table_name = 'pedidos_venda'
                """).fetchone()[0]
                new_exists = con.execute(f"""
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema = '{schema}' AND table_name = 'venda_direta'
                """).fetchone()[0]
                if old_exists and not new_exists:
                    logger.info(f"Renaming {schema}.pedidos_venda -> {schema}.venda_direta...")
                    con.execute(f"ALTER TABLE {schema}.pedidos_venda RENAME TO venda_direta")

            promote_notas(con)
            promote_pedidos(con)
            promote_venda_direta(con)
            promote_pagamentos(con)
            promote_full_load(con, "empresas",   ["M0_CODIGO", "M0_CODFIL"])
            promote_full_load(con, "tes",        ["F4_CODIGO"])
            promote_full_load(con, "produtos",   ["B1_COD"])
            promote_full_load(con, "clientes",   ["A1_COD", "A1_LOJA"], keep_all=True)
            promote_full_load(con, "vendedores", ["A3_COD"])

            # Generate the unmapped columns audit report
            generate_unmapped_columns_report(con)

            logger.info("=== PROTHEUS BRONZE TO SILVER PROMOTION FINISHED SUCCESSFUL ===")
    except Exception as e:
        logger.error(f"Silver Promotion Failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
