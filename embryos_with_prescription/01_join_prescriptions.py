#!/usr/bin/env python3
"""
01_join_prescriptions.py

Joins gold.planilha_embryoscope_combined (filtered) with
clinisys_all.silver.view_medicamentos_prescricoes to produce a long-format
gold table: one row per (embryo × prescription).

Join key: trat1_id (lake) = ficha_id (prescriptions)

Filter applied to source embryo table:
  1. oocito_TCD = 'Transferido'  OR  descong_em_DataTransferencia IS NOT NULL
  2. outcome_type (trimmed + lowercased) NOT IN excluded values and NOT NULL

Output: huntington_data_lake.gold.embryos_with_prescription_long
"""

import duckdb as db
import os
import logging
from datetime import datetime

# ── Logging setup ──────────────────────────────────────────────────────────────
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

# ── Database paths ─────────────────────────────────────────────────────────────
REPO_ROOT  = os.path.dirname(os.path.dirname(__file__))
LAKE_DB    = os.path.join(REPO_ROOT, 'database', 'huntington_data_lake.duckdb')
CLINISYS_DB = os.path.join(REPO_ROOT, 'database', 'clinisys_all.duckdb')

# ── outcome_type exclusion list (compared after TRIM + LOWER + collapse spaces)
EXCLUDED_OUTCOME_TYPES = [
    "total cryopreservation",
    "sem respost",
    "sem contato",
    "other",
    "due to lack of normal embryos",
]

# ── Prescription columns to include (only the enriched set from feature_engineering)
PRESCRIPTION_COLUMNS = [
    "p.id              AS presc_id",
    "p.ficha_id        AS presc_ficha_id",
    "p.medicamento     AS presc_medicamento",
    "p.med_nome        AS presc_med_nome",
    "p.dose            AS presc_dose",
    "p.unidade         AS presc_unidade",
    "p.unidade_padronizada AS presc_unidade_padronizada",
    "p.intervalo       AS presc_intervalo",
    "p.data_inicial    AS presc_data_inicial",
    "p.data_final      AS presc_data_final",
    "p.numero_dias     AS presc_numero_dias",
    "p.dose_total      AS presc_dose_total",
    "p.grupo_medicamento AS presc_grupo_medicamento",
]


def normalize_outcome(col: str) -> str:
    """Return a SQL expression that normalises outcome_type for comparison:
    trim whitespace, collapse multiple spaces, lowercase."""
    return f"TRIM(REGEXP_REPLACE(LOWER(CAST({col} AS VARCHAR)), '\\s+', ' ', 'g'))"


def get_exclusion_clause(col: str) -> str:
    """Return a WHERE condition that excludes NULL and the excluded outcome types."""
    norm = normalize_outcome(col)
    quoted = ", ".join(f"'{v}'" for v in EXCLUDED_OUTCOME_TYPES)
    return f"({col} IS NOT NULL AND {norm} NOT IN ({quoted}))"


def build_filter_sql() -> str:
    """Return the full WHERE clause for the embryo source table."""
    transfer_cond  = "(e.oocito_TCD = 'Transferido' OR e.descong_em_DataTransferencia IS NOT NULL)"
    outcome_cond   = get_exclusion_clause("e.outcome_type")
    return f"{transfer_cond}\n    AND {outcome_cond}"


def get_connection():
    """Open huntington_data_lake (RW) and attach clinisys_all (RO)."""
    conn = db.connect(LAKE_DB, read_only=False)
    conn.execute(f"ATTACH '{CLINISYS_DB}' AS clinisys (READ_ONLY)")
    logger.info(f"Connected to {LAKE_DB}")
    logger.info(f"Attached    {CLINISYS_DB} as clinisys")
    return conn


def create_long_table(conn):
    """Build gold.embryos_with_prescription_long."""

    filter_sql = build_filter_sql()
    presc_cols_sql = ",\n    ".join(PRESCRIPTION_COLUMNS)

    # ── 1. Row count before/after filter ─────────────────────────────────────
    total_source = conn.execute(
        "SELECT COUNT(*) FROM gold.planilha_embryoscope_combined"
    ).fetchone()[0]

    filtered_count, filtered_oocito_ids = conn.execute(f"""
        SELECT COUNT(*), COUNT(DISTINCT oocito_id)
        FROM gold.planilha_embryoscope_combined e
        WHERE {filter_sql}
    """).fetchone()

    logger.info("=" * 60)
    logger.info("SOURCE FILTER STATS")
    logger.info("=" * 60)
    logger.info(f"  Total rows in source table    : {total_source:>10,}")
    logger.info(f"  Rows surviving filter         : {filtered_count:>10,}  ({filtered_count/total_source*100:.1f}%)")
    logger.info(f"  Distinct oocito_id after filter: {filtered_oocito_ids:>9,}")
    logger.info("=" * 60)

    # ── 2. Build the long table ───────────────────────────────────────────────
    logger.info("Building gold.embryos_with_prescription_long ...")

    conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
    conn.execute("DROP TABLE IF EXISTS gold.embryos_with_prescription_long")

    query = f"""
    CREATE TABLE gold.embryos_with_prescription_long AS
    SELECT
        e.*,
        -- Prescription columns (prefixed with presc_)
        {presc_cols_sql}
    FROM (
        SELECT *
        FROM gold.planilha_embryoscope_combined e
        WHERE {filter_sql}
    ) e
    JOIN clinisys.silver.view_medicamentos_prescricoes p
           ON e.trat1_id = p.ficha_id
    """

    conn.execute(query)

    # ── 3. Join metrics Split by External/Internal ───────────────────────────
    stats_df = conn.execute("""
        SELECT
            CASE WHEN flag_is_external_medical = 1 THEN 'EXTERNAL' ELSE 'INTERNAL' END as medical_type,
            COUNT(DISTINCT oocito_id)             AS total_embryos,
            COUNT(DISTINCT CASE WHEN presc_id IS NOT NULL THEN oocito_id END) AS matched_embryos,
            COUNT(*)                              AS total_rows,
            COUNT(CASE WHEN presc_id IS NOT NULL THEN 1 END) AS matched_rows
        FROM gold.embryos_with_prescription_long
        GROUP BY 1
        ORDER BY 1
    """).df()

    logger.info("=" * 60)
    logger.info("FILTERED JOIN STATS (INTERNAL vs EXTERNAL)")
    logger.info("=" * 60)
    
    total_matched_embryos = 0
    total_filtered_embryos = 0
    
    for _, row in stats_df.iterrows():
        m_type = row['medical_type']
        t_emb  = int(row['total_embryos'])
        m_emb  = int(row['matched_embryos'])
        t_rows = int(row['total_rows'])
        m_rows = int(row['matched_rows'])
        rate   = (m_emb / t_emb * 100) if t_emb > 0 else 0
        
        total_matched_embryos += m_emb
        total_filtered_embryos += t_emb
        
        logger.info(f"[{m_type}]")
        logger.info(f"  Distinct oocito_id              : {t_emb:>10,}")
        logger.info(f"  Matched oocito_id (>=1 presc)   : {m_emb:>10,}  ({rate:.1f}%)")
        logger.info(f"  Total rows (long format)        : {t_rows:>10,}")
        logger.info(f"  Matched rows (with presc)       : {m_rows:>10,}")
        logger.info("-" * 60)

    overall_rate = (total_matched_embryos / total_filtered_embryos * 100) if total_filtered_embryos > 0 else 0
    logger.info(f"[OVERALL]")
    logger.info(f"  Total filtered embryos          : {total_filtered_embryos:>10,}")
    logger.info(f"  Total matched embryos           : {total_matched_embryos:>10,}  ({overall_rate:.1f}%)")
    logger.info("=" * 60)

    # ── 4. Top groups ─────────────────────────────────────────────────────────
    top_groups = conn.execute("""
        SELECT presc_grupo_medicamento, COUNT(*) n
        FROM gold.embryos_with_prescription_long
        WHERE presc_grupo_medicamento IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """).df()

    logger.info("Top 10 prescription groups:")
    for _, row in top_groups.iterrows():
        logger.info(f"  {row['presc_grupo_medicamento']:<30} {int(row['n']):>8,}")
    logger.info("=" * 60)
    logger.info("gold.embryos_with_prescription_long created successfully.")


def main():
    logger.info("=== STARTING EMBRYOS WITH PRESCRIPTION JOIN ===")
    conn = None
    try:
        conn = get_connection()
        create_long_table(conn)
        logger.info("Done.")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
